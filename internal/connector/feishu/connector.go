// 飞书云文档连接器：tenant（共享文件夹遍历）+ user（已授权用户云空间）双模式。
// 变更检测：feishu_doc_copy(token × person)，某人还没有这一份、或文档/文件名变了才写给他。
// 归属：所有者 + 扫描时能看到的授权员工 + 协作者列表里的人，各存一份；谁看不到了只删谁的那份。
package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path"
	"strings"
	"sync/atomic"
	"time"

	feishuapi "github.com/gitlayzer/fastgpt-sync-adapter/internal/feishu"
	"github.com/gitlayzer/fastgpt-sync-adapter/internal/model"
	"github.com/gitlayzer/fastgpt-sync-adapter/internal/storage"
)

// Config 飞书连接器配置。
type Config struct {
	AppID     string   `yaml:"app_id"`
	AppSecret string   `yaml:"app_secret"`
	Mode      string   `yaml:"mode"`    // tenant / user / both
	Folders   []string `yaml:"folders"` // tenant 模式遍历的共享文件夹 token 列表
	// WikiSpaceIDs 用户模式下不再使用：我的文档库用 my_library 自动发现，团队库由 ListWikiSpaces 返回。
	WikiSpaceIDs []string `yaml:"wiki_space_ids"`
	SearchKeys   []string `yaml:"search_keys"` // 显式指定的共享文档搜索关键词（留空则不触发搜索，避免抓入公共模板）
	// CollabRefresh 协作者列表刷新周期（0 = 默认 6h）。加减协作者不改文档修改时间，只能定期查。
	CollabRefresh time.Duration `yaml:"collab_refresh_interval"`
}

// API 抽象（便于测试替换；feishuapi.Client 适配实现）。
type API interface {
	ListFolderChildren(token, folderToken string) ([]feishuapi.DriveFile, error)
	WalkFolder(token, folderToken string, depth int, fn func(feishuapi.DriveFile)) error
	RootFolderMeta(token string) (string, error)
	DownloadFile(token, fileToken string) (io.ReadCloser, error)
	DocxRawContent(token, documentID string) (string, error)
	DocRawContent(token, docToken string) (string, error)
	TenantAccessToken() (string, error)

	// 知识库（wiki）：飞书新版「我的文档库」只能从这里列出来
	ListWikiSpaces(token string) ([]feishuapi.WikiSpace, error)
	GetMyLibrarySpace(userToken string) (*feishuapi.WikiSpace, error)
	WalkWikiSpace(token, spaceID string, maxDepth int, fn func(feishuapi.WikiSpace, feishuapi.WikiNode)) error

	// 导出云文档源文件（docx/sheet/bitable → .docx/.xlsx）。
	// raw_content 只有纯文本会丢图片与样式，导出才能拿到完整源文件。
	ExportFile(tok, token, docType string) (string, error)
	DownloadExport(tok, fileToken string) (io.ReadCloser, error)

	// 搜索全库文档（包含他人“与我共享”的散落文档）
	SearchDocs(userToken, searchKey string, offset, count int) ([]feishuapi.SearchDocEntity, error)

	// 文档协作者列表（所有者 token 可查）
	ListPermissionMembers(userToken, token, docType string) ([]feishuapi.PermissionMember, error)

	// 妙记/智能纪要接口
	SearchMinutes(userToken string) ([]feishuapi.MinuteItem, error)
	DownloadMinutesTranscript(userToken, minuteToken string) (io.ReadCloser, error)

	// 邮箱邮件与附件接口
	ListMailboxMessages(userToken string, pageSize int) ([]string, error)
	GetMailMessage(userToken, messageID string) (*feishuapi.MailMessage, error)
	DownloadMailAttachment(userToken, messageID, attachmentID string) (io.ReadCloser, error)
}

// 限流：drive 下载约 5 QPS，FetchAttachment 前统一减速。
const downloadMinInterval = 220 * time.Millisecond

// Connector 飞书云文档连接器。
type Connector struct {
	cfg   Config
	api   API
	store *storage.Store

	tenantToken string
	pending     []model.Record        // 本轮已扫描出的待产出行列（增量）
	pendingMeta map[string]pendingDoc // 待 Ack 的文档元数据（id → 状态）
	lastDl      atomic.Int64          // 上次下载时间（纳秒），限流用
}

func New(cfg Config, store *storage.Store) *Connector {
	return &Connector{
		cfg:         cfg,
		store:       store,
		pendingMeta: map[string]pendingDoc{},
	}
}

// NewWithAPI 注入自定义 API（测试用）。
func NewWithAPI(cfg Config, store *storage.Store, api API) *Connector {
	return &Connector{
		cfg:         cfg,
		store:       store,
		api:         api,
		pendingMeta: map[string]pendingDoc{},
	}
}

func (c *Connector) Name() string { return "feishu" }

func (c *Connector) Init(ctx context.Context) error {
	if c.api == nil {
		c.api = feishuapi.New(c.cfg.AppID, c.cfg.AppSecret)
	}
	if c.mode() != "user" {
		tat, err := c.api.TenantAccessToken()
		if err != nil {
			return fmt.Errorf("tenant_access_token: %w", err)
		}
		c.tenantToken = tat
	}
	return nil
}

func (c *Connector) mode() string {
	if c.cfg.Mode == "" {
		return "tenant"
	}
	return c.cfg.Mode
}

// docToRecord 文档 → 统一 Record。owner + viewer 都进 OwnerIDs（各存一份）。
func docToRecord(f feishuapi.DriveFile, viewerID string) model.Record {
	owners := []string{}
	if f.OwnerID != "" {
		owners = append(owners, f.OwnerID)
	}
	if viewerID != "" && viewerID != f.OwnerID {
		owners = append(owners, viewerID)
	}
	meta, _ := json.Marshal(map[string]any{
		"token": f.Token, "name": f.Name, "type": f.Type,
		"owner_id": f.OwnerID, "url": f.URL, "modified_time": f.ModifiedTime,
	})
	return model.Record{
		Source:    "feishu",
		Kind:      f.Type,
		ID:        f.Token,
		Timestamp: feishuapi.UnixSeconds(f.ModifiedTime) * 1000,
		OwnerIDs:  owners,
		SenderID:  f.OwnerID, // 归属以所有者为主
		Body:      meta,
		Attachments: []model.Attachment{{
			Name:   attachmentName(f),
			FileID: attachmentFileID(f),
			UserID: viewerID, // 记录持有该文档权限的具体员工，下载时精确定向取 Token
		}},
	}
}

// attachmentFileID 附件标识：file 类型走 drive 下载；docx/doc/sheet/bitable/slides 走导出。
func attachmentFileID(f feishuapi.DriveFile) string {
	switch f.Type {
	case "file":
		return "file:" + f.Token
	case "docx":
		return "docx:" + f.Token
	case "doc":
		return "doc:" + f.Token
	case "sheet":
		return "sheet:" + f.Token
	case "bitable":
		return "bitable:" + f.Token
	case "slides":
		return "slides:" + f.Token
	}
	return f.Type + ":" + f.Token
}

// attachmentName 归档文件名。
// 格式规范：<文件名主体>_<8位Token短码>.<扩展名>
//   docx/doc      → .docx
//   sheet/bitable → .xlsx
//   slides        → .pptx
//   file          → 保留原后缀
// 末尾拼入 8 位唯一 Token，彻底杜绝同名覆盖与连锁误删死穴。
func attachmentName(f feishuapi.DriveFile) string {
	name := f.Name
	if name == "" {
		name = f.Token
	}
	base := name
	ext := path.Ext(base)

	targetExt := ext
	switch f.Type {
	case "docx", "doc":
		targetExt = ".docx"
	case "sheet", "bitable":
		targetExt = ".xlsx"
	case "slides":
		targetExt = ".pptx"
	}

	// 仅当用户在文档标题末尾显式输入了对应扩展名（含同类旧版格式 .doc/.xls/.ppt），或属于原始上传文件(file)时才剥离扩展名
	shouldTrim := false
	if f.Type == "file" {
		shouldTrim = ext != ""
	} else if ext != "" {
		switch f.Type {
		case "docx", "doc":
			shouldTrim = strings.EqualFold(ext, ".docx") || strings.EqualFold(ext, ".doc")
		case "sheet", "bitable":
			shouldTrim = strings.EqualFold(ext, ".xlsx") || strings.EqualFold(ext, ".xls")
		case "slides":
			shouldTrim = strings.EqualFold(ext, ".pptx") || strings.EqualFold(ext, ".ppt")
		default:
			shouldTrim = strings.EqualFold(ext, targetExt)
		}
	}
	if shouldTrim {
		base = strings.TrimSuffix(base, ext)
	}

	shortToken := f.Token
	if len(shortToken) > 8 {
		shortToken = shortToken[len(shortToken)-8:]
	}
	if shortToken != "" {
		return fmt.Sprintf("%s_%s%s", sanitizeFilename(base), shortToken, targetExt)
	}
	return sanitizeFilename(base) + targetExt
}

func sanitizeFilename(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	return strings.TrimSpace(s)
}

// Fetch 产出变化的文档（pending 为空时先全量扫描一遍）。
func (c *Connector) Fetch(ctx context.Context, checkpoint string, limit int) ([]model.Record, string, error) {
	if len(c.pending) == 0 {
		if err := c.scan(ctx); err != nil {
			return nil, checkpoint, err
		}
	}
	n := limit
	if n <= 0 || n > len(c.pending) {
		n = len(c.pending)
	}
	batch := c.pending[:n]
	c.pending = c.pending[n:]
	next := "done"
	if len(c.pending) > 0 {
		next = "cont"
	}
	log.Printf("[feishu] 本批产出 %d 篇文档（剩余待产出 %d）", len(batch), len(c.pending))
	return batch, next, nil
}

// scan 全量扫描（tenant 共享文件夹 + 各已授权用户云空间），汇总“每篇文档谁能看到”，
// 全部用户扫完后统一按副本状态产出写入/删除记录。
func (c *Connector) scan(ctx context.Context) error {
	mode := c.mode()
	st := newScanState()

	// tenant 模式：遍历配置的共享文件夹
	if (mode == "tenant" || mode == "both") && c.tenantToken != "" {
			for _, folder := range c.cfg.Folders {
				err := c.api.WalkFolder(c.tenantToken, folder, 10, func(f feishuapi.DriveFile) {
					c.collect(st, docToRecord(f, ""), "")
					st.globalSeen[f.Token] = true
				})
				if err != nil {
					log.Printf("[feishu] 遍历共享文件夹 %s 失败: %v", folder, err)
				}
			}
	}

	// user 模式：每个已授权用户
	if mode == "user" || mode == "both" {
		grants, err := c.store.ListOAuthGrants("feishu")
		if err != nil {
			return err
		}
		now := time.Now().Unix()
				for _, g := range grants {
					if g.Status != "authorized" || g.AccessToken == "" {
						log.Printf("[feishu] 用户 %s 未处于已授权状态（status=%s），跳过", g.ExternalUserID, g.Status)
						st.inactiveUsers[g.ExternalUserID] = true // 没扫：整篇删除时保留他那份，重新授权后再判断
						continue
					}
					if g.ExpireAt <= now {
						log.Printf("[feishu] 用户 %s 授权已过期（已于 %s 到期），跳过待刷新",
							g.ExternalUserID, time.Unix(g.ExpireAt, 0).Format("2006-01-02 15:04:05"))
						st.failedUsers[g.ExternalUserID] = true // 没扫到：他持有副本的文档本轮不做整篇删除
						continue
					}

				userSeen := map[string]bool{}
				userScanFailed := false
				owned := map[string]string{} // 本人是所有者的文档：token → 类型（查协作者列表用）
				see := func(rec model.Record, typ string) {
					userSeen[rec.ID] = true
					c.collect(st, rec, g.ExternalUserID)
					if rec.SenderID == g.ExternalUserID && typ != "" {
						owned[rec.ID] = typ
					}
				}

				// a) 云空间（drive）：老式文件夹结构。
				root, err := c.api.RootFolderMeta(g.AccessToken)
				if err != nil {
					log.Printf("[feishu] 拿 %s 根目录失败: %v", g.ExternalUserID, err)
					userScanFailed = true
				} else {
					driveN := 0
					err = c.api.WalkFolder(g.AccessToken, root, 10, func(f feishuapi.DriveFile) {
						driveN++
						see(docToRecord(f, g.ExternalUserID), f.Type)
					})
					if err != nil {
						log.Printf("[feishu] 遍历 %s 云空间失败: %v", g.ExternalUserID, err)
						userScanFailed = true
					} else if driveN == 0 {
						log.Printf("[feishu] %s 云空间：0 个条目", g.ExternalUserID)
					} else {
						log.Printf("[feishu] %s 云空间：%d 个条目", g.ExternalUserID, driveN)
					}
				}

				// b) 团队知识库空间 + 个人“我的文档库”自动发现。
				// 用户模式不再使用 cfg.WikiSpaceIDs：别人的个人文档库会报无权限，
				// 被算作扫描失败后会让这个人的删除同步永远不执行。
				spaceIDs := []string{}
				myLib, err := c.api.GetMyLibrarySpace(g.AccessToken)
				if err != nil {
					log.Printf("[feishu] 取 %s 的我的文档库失败: %v", g.ExternalUserID, err)
					userScanFailed = true
				} else if myLib != nil && myLib.SpaceID != "" {
					spaceIDs = append(spaceIDs, myLib.SpaceID)
				}
				spaces, err := c.api.ListWikiSpaces(g.AccessToken)
				if err != nil {
					log.Printf("[feishu] 列 %s 知识库失败: %v", g.ExternalUserID, err)
					userScanFailed = true
				} else {
					for _, sp := range spaces {
						spaceIDs = append(spaceIDs, sp.SpaceID)
					}
				}

				total := 0
				seenSpace := map[string]bool{}
				for _, sid := range spaceIDs {
					if sid == "" || seenSpace[sid] {
						continue
					}
					seenSpace[sid] = true
					n := 0
					err := c.api.WalkWikiSpace(g.AccessToken, sid, 10, func(_ feishuapi.WikiSpace, node feishuapi.WikiNode) {
						if rec, ok := wikiNodeToRecord(node, g.ExternalUserID); ok {
							n++
							see(rec, node.ObjType)
						}
					})
					if err != nil {
						log.Printf("[feishu] 遍历知识库 %s 失败: %v", sid, err)
						userScanFailed = true
						continue
					}
					total += n
					if n > 0 {
						log.Printf("[feishu] 知识库 %s：%d 篇", sid, n)
					}
				}
				if total > 0 {
					log.Printf("[feishu] %s 知识库：%d 个空间，%d 篇文档", g.ExternalUserID, len(seenSpace), total)
				}

				// c) 显式搜索关键字扩展（如配置了业务关键词，且自动过滤系统公共模板）
				if len(c.cfg.SearchKeys) > 0 {
					searchCount := 0
					for _, key := range c.cfg.SearchKeys {
						entities, err := c.api.SearchDocs(g.AccessToken, key, 0, 50)
						if err != nil {
							continue
						}
						for _, d := range entities {
							if d.Token == "" || d.OwnerID == "" {
								continue // 过滤无所有者的公共系统模板
							}
							f := feishuapi.DriveFile{
								Token:   d.Token,
								Name:    d.Title,
								Type:    d.Type,
								OwnerID: d.OwnerID,
							}
							see(docToRecord(f, g.ExternalUserID), d.Type)
							searchCount++
						}
					}
					if searchCount > 0 {
						log.Printf("[feishu] %s 业务关键字搜索命中 %d 篇条目", g.ExternalUserID, searchCount)
					}
				}

				// d) 智能纪要/妙记扫描（SearchMinutes 会翻完所有页，列表完整，可参与删除判断）。
				// 接口失败时不算整个人扫描失败（没开通妙记的账号可能一直报错），只是本轮不判断妙记的删除。
				minutesList, err := c.api.SearchMinutes(g.AccessToken)
				minutesOK := err == nil
				if err != nil {
					log.Printf("[feishu] 取 %s 的妙记失败（本轮不判断妙记删除）: %v", g.ExternalUserID, err)
				}
				if err == nil && len(minutesList) > 0 {
					for _, m := range minutesList {
						if m.MinuteToken == "" {
							continue
						}
						title := m.Title
						if title == "" {
							title = "未命名智能纪要"
						}
						rec := model.Record{
							Source:    "feishu",
							Kind:      "minutes",
							ID:        m.MinuteToken,
							Timestamp: m.CreateTime,
							OwnerIDs:  []string{m.OwnerID, g.ExternalUserID},
							SenderID:  m.OwnerID,
							Attachments: []model.Attachment{{
								Name:   title + ".md",
								FileID: "minutes:" + m.MinuteToken,
								UserID: g.ExternalUserID,
							}},
						}
						see(rec, "")
					}
					log.Printf("[feishu] %s 扫描到 %d 篇智能纪要/妙记", g.ExternalUserID, len(minutesList))
				}

					// e) 邮箱邮件附件扫描（每次最多取 20 封，超出飞书上限会被拒绝；列表不参与删除判断）
					msgIDs, err := c.api.ListMailboxMessages(g.AccessToken, 20)
					if err != nil {
						log.Printf("[feishu] 获取用户 %s 邮箱邮件列表失败: %v", g.ExternalUserID, err)
					} else if len(msgIDs) > 0 {
						mailCount := 0
						for _, msgID := range msgIDs {
							if msgID == "" {
								continue
							}
							msg, err := c.api.GetMailMessage(g.AccessToken, msgID)
							if err != nil {
								log.Printf("[feishu] 获取邮件 %s 详情失败: %v", msgID, err)
								continue
							}
							if msg == nil || len(msg.Attachments) == 0 {
								continue
							}
							var recordAtts []model.Attachment
							usedNames := map[string]int{} // 同一封邮件里的同名附件再加序号
							for _, ma := range msg.Attachments {
								if ma.ID == "" || ma.Name == "" || ma.IsInline {
									continue
								}
								// 附件名后加 messageID 短码，避免不同邮件的同名附件互相覆盖
								shortID := msg.MessageID
								if len(shortID) > 8 {
									shortID = shortID[len(shortID)-8:]
								}
								ext := path.Ext(ma.Name)
								attName := sanitizeFilename(strings.TrimSuffix(ma.Name, ext)) + "_" + shortID
								if usedNames[attName]++; usedNames[attName] > 1 {
									attName = fmt.Sprintf("%s_%d", attName, usedNames[attName])
								}
								recordAtts = append(recordAtts, model.Attachment{
									Name:   attName + ext,
									FileID: "mail:" + msg.MessageID + ":" + ma.ID,
									Size:   ma.Size,
									UserID: g.ExternalUserID,
								})
							}
							if len(recordAtts) > 0 {
								rec := model.Record{
									Source:      "feishu",
									Kind:        "mail",
									ID:          "mail_" + msg.MessageID,
									Timestamp:   msg.CreatedTime,
									OwnerIDs:    []string{g.ExternalUserID},
									SenderID:    g.ExternalUserID,
									Attachments: recordAtts,
								}
							c.collect(st, rec, g.ExternalUserID)
							mailCount++
						}
					}
					if mailCount > 0 {
						log.Printf("[feishu] %s 扫描到 %d 封含附件邮件", g.ExternalUserID, mailCount)
					}
				}

				// f) 本人是所有者的文档：按周期刷新协作者列表，把协作者加进“能看到的人”。
				// 加减协作者不会改变文档修改时间（实测），所以不能只在文档变化时查。
				c.refreshCollaborators(st, g.AccessToken, owned, now)

				if userScanFailed {
					st.failedUsers[g.ExternalUserID] = true
					log.Printf("[feishu] 用户 %s 扫描过程存在接口错误，触发熔断保护，暂停该用户本轮删除同步", g.ExternalUserID)
				} else {
					st.scanned = append(st.scanned, scannedUser{id: g.ExternalUserID, seen: userSeen, minutesOK: minutesOK})
					for t := range userSeen {
						st.globalSeen[t] = true
					}
				}
			}
		}

		c.emitWrites(st)
		c.emitDeletes(st)
		log.Printf("[feishu] 扫描完成，待产出 %d 篇", len(c.pending))
		return nil
	}

// wikiNodeToRecord wiki 节点 → Record。
//
// 支持的源类型：
//   file  上传的文件 → 直接下载原始字节（无损）
//   docx  新版文档   → 导出为 .docx（保留格式与内嵌图片，优于纯文本）
//   doc   旧版文档   → 导出为 .docx
//   sheet 电子表格   → 导出为 .xlsx
//   bitable 多维表格 → 导出为 .xlsx
//
// 原则：一律取「源文件」而非转换后的文本，避免丢内容。
func wikiNodeToRecord(n feishuapi.WikiNode, viewerID string) (model.Record, bool) {
	switch n.ObjType {
	case "file", "docx", "doc", "sheet", "bitable", "slides":
	default:
		return model.Record{}, false
	}
	token := n.ObjToken
	if token == "" {
		return model.Record{}, false
	}
	editTime := n.ObjEditTime
	if editTime == "" {
		editTime = n.NodeEditTime
	}
	if editTime == "" {
		editTime = n.ObjCreateTime
	}
	if editTime == "" {
		editTime = n.NodeCreateTime
	}
	f := feishuapi.DriveFile{
		Token:        token,
		Name:         n.Title,
		Type:         n.ObjType,
		ModifiedTime: editTime,
	}
	// wiki 节点优先取 owner，次选 creator；仍为空时以当前授权查看人兜底
	ownerID := n.Owner
	if ownerID == "" {
		ownerID = n.Creator
	}
	if ownerID == "" {
		ownerID = viewerID
	}
	f.OwnerID = ownerID
	return docToRecord(f, viewerID), true
}

// 副本来源：所有者本人 / 授权员工扫描时能看到 / 协作者列表。
const (
	viaOwner  = "owner"
	viaScan   = "scan"
	viaCollab = "collab"
)

// collabRefreshDefault 协作者列表默认刷新周期（cfg.CollabRefresh 为 0 时使用）。
const collabRefreshDefault = 6 * time.Hour

// wantDoc 本轮扫描汇总出的一篇文档：哪些人应该在自己目录下有一份。
type wantDoc struct {
	rec     model.Record
	editTs  int64
	owner   string
	persons []string          // 保持加入顺序：所有者在前
	via     map[string]string // person → 来源
	reader  string            // 下载用谁的 token：优先所有者本人，否则第一个能看到它的授权员工
}

func (w *wantDoc) addPerson(person, via string) {
	if person == "" {
		return
	}
	if old, ok := w.via[person]; ok {
		// 来源优先级 owner > scan > collab
		if old == viaOwner || (old == viaScan && via == viaCollab) {
			return
		}
		w.via[person] = via
		return
	}
	w.via[person] = via
	w.persons = append(w.persons, person)
}

type scannedUser struct {
	id        string
	seen      map[string]bool
	minutesOK bool // 本轮妙记列表取成功；失败时不判断妙记删除
}

// scanState 一轮扫描的汇总结果。
type scanState struct {
	want        map[string]*wantDoc
	order       []string                   // 文档首次出现的顺序（产出顺序）
	scanned     []scannedUser              // 本轮扫描完全成功的授权员工
	globalSeen  map[string]bool            // 所有扫描成功的员工看到的文档并集
	failedUsers map[string]bool            // 本轮扫描失败或被跳过的员工
	inactiveUsers map[string]bool          // 有授权记录但当前不可用（未授权/已失效）的员工
	collabFresh map[string]map[string]bool // 本轮刷新成功的协作者列表：token → open_id 集合
}

func newScanState() *scanState {
	return &scanState{
		want:        map[string]*wantDoc{},
		globalSeen:  map[string]bool{},
		failedUsers: map[string]bool{},
		inactiveUsers: map[string]bool{},
		collabFresh: map[string]map[string]bool{},
	}
}

// collect 把“viewer 能看到这篇文档”合并进本轮汇总（不再按 token 全局去重丢掉后来的查看人）。
// 是否需要写入、写给谁，等全部用户扫完后由 emitWrites 按副本状态统一判断。
func (c *Connector) collect(st *scanState, rec model.Record, viewer string) {
	w, ok := st.want[rec.ID]
	if !ok {
		w = &wantDoc{rec: rec, editTs: rec.Timestamp / 1000, owner: rec.SenderID, via: map[string]string{}}
		st.want[rec.ID] = w
		st.order = append(st.order, rec.ID)
	}
	if ts := rec.Timestamp / 1000; ts > w.editTs {
		w.editTs = ts
	}
	w.addPerson(w.owner, viaOwner)
	if viewer != "" && viewer != w.owner {
		w.addPerson(viewer, viaScan)
	}
	if viewer != "" && (w.reader == "" || viewer == w.owner) {
		w.reader = viewer
	}
}

// refreshCollaborators 对本人是所有者的文档，按周期（或文档有变化时）刷新协作者列表；
// 不刷新的沿用上次查到的列表。只取 member_type=openid 的个人，群/部门不展开。
func (c *Connector) refreshCollaborators(st *scanState, userToken string, owned map[string]string, now int64) {
	every := c.cfg.CollabRefresh
	if every <= 0 {
		every = collabRefreshDefault
	}
	for token, typ := range owned {
		w := st.want[token]
		if w == nil {
			continue
		}
		checkedAt, members := c.store.GetDocMembers(token)
		changed := true
		if copies, err := c.store.ListCopies(token); err == nil {
			for _, cp := range copies {
				if cp.PersonID == w.owner && cp.EditTime >= w.editTs {
					changed = false
				}
			}
		}
		if changed || now-checkedAt >= int64(every/time.Second) {
			c.throttle()
			items, err := c.api.ListPermissionMembers(userToken, token, typ)
			if err != nil {
				log.Printf("[feishu] 查 %s 协作者失败（沿用上次结果）: %v", token, err)
			} else {
				fresh := map[string]bool{}
				members = members[:0]
				for _, m := range items {
					if m.MemberType == "openid" && m.MemberID != "" {
						fresh[m.MemberID] = true
						members = append(members, m.MemberID)
					}
				}
				st.collabFresh[token] = fresh
				if err := c.store.SetDocMembers(token, now, members); err != nil {
					log.Printf("[feishu] 记录 %s 协作者失败: %v", token, err)
				}
			}
		}
		for _, p := range members {
			w.addPerson(p, viaCollab)
		}
	}
}

// emitWrites 按副本状态产出写入记录：某人还没有这一份、或文档变了、或文件名变了，才写给他。
func (c *Connector) emitWrites(st *scanState) {
	for _, token := range st.order {
		w := st.want[token]
		name := attachmentNameOf(w.rec)
		have := map[string]storage.DocCopy{}
		if copies, err := c.store.ListCopies(token); err == nil {
			for _, cp := range copies {
				have[cp.PersonID] = cp
			}
		}
		var need []string
		prev := ""
		for _, p := range w.persons {
			cp, ok := have[p]
			if ok && cp.FileName != "" && cp.FileName != name && prev == "" {
				prev = cp.FileName
			}
			if !ok || (w.editTs > 0 && cp.EditTime < w.editTs) || cp.FileName != name {
				need = append(need, p)
				continue
			}
			if cp.Via != w.via[p] { // 内容没变、只是来源变了：只更新状态，不重写文件
				cp.Via = w.via[p]
				_ = c.store.UpsertCopy(cp)
			}
		}
		if len(need) == 0 {
			continue
		}
		rec := w.rec
		rec.OwnerIDs = need
		rec.Attachments = append([]model.Attachment(nil), w.rec.Attachments...)
		if w.reader != "" {
			for i := range rec.Attachments {
				rec.Attachments[i].UserID = w.reader
			}
		}
			if prev != "" && rec.Kind != "mail" && len(rec.Attachments) > 0 {
				rec.Attachments[0].PrevName = prev
			}
		c.pending = append(c.pending, rec)

		// 这里只入队，不写状态：Sink 全部写入成功后由 Ack 逐人记录副本，
		// 否则写 NAS 失败时下一轮会被判为「已有」而永久跳过。
		vias := map[string]string{}
		for _, p := range need {
			vias[p] = w.via[p]
		}
		c.pendingMeta[rec.ID] = pendingDoc{
			token:    token,
			kind:     w.rec.Kind,
			title:    name,
			ownerID:  w.owner,
			editTime: w.editTs,
			persons:  vias,
		}
	}
}

// emitDeletes 产出删除记录。只对本轮扫描完全成功的员工判断，原熔断保留：
//   - 本人看不到了、但还有别人看得到：只删本人那份（权限收回）；本人是所有者时保留
//     （所有者的文档可能放在别人的文件夹里，本人云盘树下本来就列不到）
//   - 本轮所有扫描成功的员工都看不到了：整篇删除（所有副本），前提是持有副本的人本轮都扫描成功
//   - 协作者列表刷新后某人不在了：只删他那份
// 邮件只取最近 50 封，列表不完整，不参与删除判断。
func (c *Connector) emitDeletes(st *scanState) {
	dels := map[string]bool{}
	del := func(cp storage.DocCopy) {
		key := cp.Token + "\x00" + cp.PersonID
		if dels[key] {
			return
		}
		dels[key] = true
		id := "del_" + cp.Token + "_" + cp.PersonID
		c.pending = append(c.pending, model.Record{
			Source:   "feishu",
			Kind:     "delete",
			ID:       id,
			SenderID: cp.OwnerID,
			OwnerIDs: []string{cp.PersonID},
			Attachments: []model.Attachment{{
				Name:   cp.FileName,
				FileID: cp.Token,
				UserID: cp.PersonID,
			}},
		})
		c.pendingMeta[id] = pendingDoc{token: cp.Token, deletePerson: cp.PersonID, isDelete: true}
		log.Printf("[feishu] %s 的副本需删除: %s (%s)", cp.PersonID, cp.FileName, cp.Token)
	}

	lost := map[string]bool{} // 本轮没有任何人看到的文档
	for _, u := range st.scanned {
		copies, err := c.store.ListCopiesByPerson(u.id)
		if err != nil {
			continue
		}
		var mine, missing []storage.DocCopy
		for _, cp := range copies {
			if cp.Kind == "mail" || strings.HasPrefix(cp.Token, "mail_") || (cp.Via != viaOwner && cp.Via != viaScan) {
				continue
			}
			if cp.Kind == "minutes" && !u.minutesOK {
				continue
			}
			mine = append(mine, cp)
			if u.seen[cp.Token] || (cp.Via == viaOwner && st.globalSeen[cp.Token]) {
				continue
			}
			missing = append(missing, cp)
		}
		// 熔断保护：单次扫描缺失比例超过 50%（且历史文档 > 5），防止接口异常大面积误删
		if len(mine) > 5 && len(missing) > len(mine)/2 {
			log.Printf("[feishu] 警告：用户 %s 缺失文档比例过高 (%d/%d)，触发熔断防误删保护，跳过本轮删除",
				u.id, len(missing), len(mine))
			continue
		}
		for _, cp := range missing {
			if st.globalSeen[cp.Token] {
				del(cp) // 别人还看得到：只删本人那份
				continue
			}
			lost[cp.Token] = true
		}
	}

	for token := range lost {
		copies, err := c.store.ListCopies(token)
		if err != nil {
			continue
		}
		blocked := false
		for _, cp := range copies {
			if (cp.Via == viaOwner || cp.Via == viaScan) && st.failedUsers[cp.PersonID] {
				blocked = true // 有持有人本轮没扫成功：可能只是没扫到，下轮再判断
				break
			}
		}
		if blocked {
			continue
		}
		for _, cp := range copies {
			if (cp.Via == viaOwner || cp.Via == viaScan) && st.inactiveUsers[cp.PersonID] {
				continue // 授权失效的员工本轮没扫，分不清是文档被删还是别人失去权限：他那份先留着
			}
			del(cp)
		}
	}

	for token, fresh := range st.collabFresh {
		if copies, err := c.store.ListCopies(token); err == nil {
			for _, cp := range copies {
				if cp.Via == viaCollab && !fresh[cp.PersonID] {
					del(cp)
				}
			}
		}
	}
}

// pendingDoc 待确认记录的元数据（Ack 时写副本状态用）。
type pendingDoc struct {
	token        string
	kind         string
	title        string
	ownerID      string
	editTime     int64
	persons      map[string]string // 写入记录：person → 来源
	isDelete     bool
	deletePerson string // 删除记录：删的是谁的那份
}

// Ack 实现 connector.AckConnector：Sink 写入成功后记录副本状态。
func (c *Connector) Ack(recordID string) error {
	meta, ok := c.pendingMeta[recordID]
	if !ok {
		return nil // 非本连接器产出的记录，忽略
	}
	delete(c.pendingMeta, recordID)
	if meta.isDelete {
		if err := c.store.DeleteCopy(meta.token, meta.deletePerson); err != nil {
			return err
		}
		if rest, err := c.store.ListCopies(meta.token); err == nil && len(rest) == 0 {
			return c.store.DeleteDocState(meta.token)
		}
		return nil
	}
	for p, via := range meta.persons {
		if err := c.store.UpsertCopy(storage.DocCopy{
			Token: meta.token, PersonID: p, Via: via,
			EditTime: meta.editTime, FileName: meta.title, OwnerID: meta.ownerID, Kind: meta.kind,
		}); err != nil {
			return err
		}
	}
	return c.store.UpsertDocState(meta.token, meta.title, meta.ownerID, meta.editTime)
}

// attachmentNameOf 取 Record 里第一个附件的名字（写 doc_state 用）。
func attachmentNameOf(rec model.Record) string {
	if len(rec.Attachments) > 0 {
		return rec.Attachments[0].Name
	}
	return ""
}

// FetchAttachment 拉取文档内容（file 原始字节 / docx·doc 纯文本），带限流。
// FetchAttachment 取云文档的「源文件」。
//
//   file            直接下载原始字节（上传的文件，无损）
//   docx/doc        导出为 .docx（保留图片、表格、样式；raw_content 只有纯文本会丢内容）
//   sheet/bitable   导出为 .xlsx
//
// 统一用 user token：导出接口要求调用者对文档有阅读权限，
// tenant token 需要在每篇文档上单独「添加文档应用」，运维成本过高。
func (c *Connector) FetchAttachment(ctx context.Context, a *model.Attachment) (io.ReadCloser, error) {
	c.throttle()
	scheme, token, _ := strings.Cut(a.FileID, ":")
	// 定向取 Token：优先使用该附件扫描/属主用户的 Token，消除首人 Token 冒用
	tok := ""
	if a.UserID != "" && c.store != nil {
		if g, err := c.store.GetOAuthGrant("feishu", a.UserID); err == nil && g.Status == "authorized" && g.ExpireAt > time.Now().Unix() {
			tok = g.AccessToken
		}
	}
	if tok == "" {
		tok = c.userToken() // 兜底：取第一个有效用户 Token
	}
	if tok == "" {
		tok = c.tenantToken
	}

	switch scheme {
	case "file":
		return c.api.DownloadFile(tok, token)
	case "docx", "doc", "sheet", "bitable", "slides":
		// 导出是异步的（创建→轮询→下载），期间要响应取消
		type res struct {
			rc  io.ReadCloser
			err error
		}
		ch := make(chan res, 1)
		go func() {
			fileToken, err := c.api.ExportFile(tok, token, scheme)
			if err != nil {
				ch <- res{nil, err}
				return
			}
			rc, err := c.api.DownloadExport(tok, fileToken)
			ch <- res{rc, err}
		}()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-ch:
			return r.rc, r.err
		}
	case "minutes":
		rc, err := c.api.DownloadMinutesTranscript(tok, token)
		if err != nil {
			return nil, err
		}
		raw, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			return nil, readErr
		}
		var buf strings.Builder
		buf.WriteString("# " + strings.TrimSuffix(a.Name, ".md") + "\n\n")
		buf.WriteString("> 来源：飞书智能会议纪要 / 妙记\n\n")
		buf.WriteString("## 逐字稿与发言记录\n\n")
		buf.Write(raw)
		return io.NopCloser(strings.NewReader(buf.String())), nil
	case "mail":
		// fileID 格式为 mail:<messageID>:<attachmentID>
		parts := strings.Split(token, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("非法邮件附件标识: %s", token)
		}
		return c.api.DownloadMailAttachment(tok, parts[0], parts[1])
	}
	return nil, fmt.Errorf("暂不支持的文档类型: %s", scheme)
}

// userToken 取当前可用的用户 token（导出等接口需要用户身份）。
func (c *Connector) userToken() string {
	grants, err := c.store.ListOAuthGrants("feishu")
	if err != nil || len(grants) == 0 {
		return ""
	}
	now := time.Now().Unix()
	for _, g := range grants {
		if g.Status == "authorized" && g.AccessToken != "" && g.ExpireAt > now {
			return g.AccessToken
		}
	}
	return ""
}

// throttle 下载限速（≥220ms 间隔 ≈ 4.5 QPS）。
func (c *Connector) throttle() {
	now := time.Now().UnixNano()
	last := c.lastDl.Load()
	if wait := downloadMinInterval - time.Duration(now-last); wait > 0 && last > 0 {
		time.Sleep(wait)
	}
	c.lastDl.Store(time.Now().UnixNano())
}

func (c *Connector) Close() error { return nil }
