package feishu

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	feishuapi "github.com/gitlayzer/fastgpt-sync-adapter/internal/feishu"
	"github.com/gitlayzer/fastgpt-sync-adapter/internal/model"
	"github.com/gitlayzer/fastgpt-sync-adapter/internal/storage"
)

// copyAPI 在 mockAPI 基础上可注入：邮件/妙记、我的文档库、知识库遍历失败。
type copyAPI struct {
	*mockAPI
	mailIDs  []string
	mailMap  map[string]*feishuapi.MailMessage
	minutes  []feishuapi.MinuteItem
	minErr   error
	myLib    map[string]string // userToken -> 我的文档库 space_id
	myLibErr error
	walkErr  map[string]bool // spaceID -> 遍历报错
}

func newCopyAPI() *copyAPI {
	return &copyAPI{
		mockAPI:  newMockAPI(),
		mailMap:  map[string]*feishuapi.MailMessage{},
		myLib:    map[string]string{},
		walkErr:  map[string]bool{},
	}
}

func (m *copyAPI) ListMailboxMessages(string, int) ([]string, error) {
	return m.mailIDs, nil
}
func (m *copyAPI) GetMailMessage(tok, msgID string) (*feishuapi.MailMessage, error) {
	if m.mailMap != nil {
		return m.mailMap[msgID], nil
	}
	return nil, nil
}
func (m *copyAPI) SearchMinutes(string) ([]feishuapi.MinuteItem, error) { return m.minutes, m.minErr }
func (m *copyAPI) GetMyLibrarySpace(tok string) (*feishuapi.WikiSpace, error) {
	if m.myLibErr != nil {
		return nil, m.myLibErr
	}
	if id := m.myLib[tok]; id != "" {
		return &feishuapi.WikiSpace{SpaceID: id}, nil
	}
	return nil, nil
}
func (m *copyAPI) WalkWikiSpace(token, spaceID string, d int, fn func(feishuapi.WikiSpace, feishuapi.WikiNode)) error {
	if m.walkErr[spaceID] {
		return errors.New("飞书错误 code=131006 msg=permission denied")
	}
	return m.mockAPI.WalkWikiSpace(token, spaceID, d, fn)
}

// copyStore 建库并为 users 依次发授权（token 为 ut0、ut1…）。
func copyStore(t *testing.T, users ...string) *storage.Store {
	t.Helper()
	st, err := storage.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for i, u := range users {
		st.UpsertOAuthGrant(&storage.OAuthGrant{
			Platform: "feishu", ExternalUserID: u, AccessToken: "ut" + string(rune('0'+i)), RefreshToken: "rt",
			ExpireAt: time.Now().Unix() + 3600, RefreshExpireAt: time.Now().Unix() + 86400, Status: "authorized",
		})
	}
	return st
}

// round 跑一轮并全部 Ack（模拟 Sink 全部写入成功）。
func round(t *testing.T, c *Connector) []model.Record {
	t.Helper()
	recs, _, err := c.Fetch(context.Background(), "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := c.Ack(r.ID); err != nil {
			t.Fatal(err)
		}
	}
	return recs
}

func kinds(recs []model.Record) []string {
	var out []string
	for _, r := range recs {
		out = append(out, r.Kind+":"+r.ID)
	}
	sort.Strings(out)
	return out
}

func copyPersons(t *testing.T, st *storage.Store, token string) []string {
	t.Helper()
	cs, err := st.ListCopies(token)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, c := range cs {
		out = append(out, c.PersonID)
	}
	sort.Strings(out)
	return out
}

func newUserConn(t *testing.T, st *storage.Store, api API, cfg Config) *Connector {
	t.Helper()
	cfg.Mode = "user"
	c := NewWithAPI(cfg, st, api)
	if err := c.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

func doc(token, name, owner, mt string) feishuapi.DriveFile {
	return feishuapi.DriveFile{Token: token, Name: name, Type: "docx", OwnerID: owner, ModifiedTime: mt}
}

// 第 1 条：邮件、妙记内容不变时不再反复删除重下；妙记消失要删，邮件消失不删。
func TestCopyMailAndMinutesNoFlapping(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "文档1", "ou_a", "100"), doc("d2", "文档2", "ou_a", "100"), doc("d3", "文档3", "ou_a", "100")}
	api.mailIDs = []string{"m1"}
	api.mailMap["m1"] = &feishuapi.MailMessage{
		MessageID:   "m1",
		CreatedTime: 1700000000000,
		Attachments: []feishuapi.MailAttachment{{ID: "a1", Name: "合同.pdf"}},
	}
	api.minutes = []feishuapi.MinuteItem{{MinuteToken: "min1", Title: "周会", OwnerID: "ou_a", CreateTime: 1700000000000}}
	c := newUserConn(t, st, api, Config{})

	if got := round(t, c); len(got) != 5 {
		t.Fatalf("首轮应产出 3 篇文档 + 1 封邮件 + 1 篇妙记，实际 %v", kinds(got))
	}
	for i := 2; i <= 3; i++ {
		if got := round(t, c); len(got) != 0 {
			t.Fatalf("第 %d 轮内容未变应无产出，实际 %v", i, kinds(got))
		}
	}
	api.minutes, api.mailIDs, api.mailMap = nil, nil, nil
	got := round(t, c)
	if len(got) != 1 || got[0].ID != "del_min1_ou_a" {
		t.Fatalf("妙记消失应只删妙记、邮件消失不删，实际 %v", kinds(got))
	}
}

// 第 2 条：改标题后，写入记录带上旧文件名，Sink 据此删除旧文件。
func TestCopyRenameCarriesPrevName(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("docRename01", "旧标题", "ou_a", "100")}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.folders["rootA"] = []feishuapi.DriveFile{doc("docRename01", "新标题", "ou_a", "200")}
	got := round(t, c)
	if len(got) != 1 {
		t.Fatalf("改标题应产出 1 条写入，实际 %v", kinds(got))
	}
	a := got[0].Attachments[0]
	if a.Name != "新标题_Rename01.docx" || a.PrevName != "旧标题_Rename01.docx" {
		t.Fatalf("文件名/旧文件名不对: %+v", a)
	}
}

// 第 3 条：配置里别人的文档库（读不了）不再影响本人的删除同步。
func TestCopyForeignWikiSpaceIgnored(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "文档1", "ou_a", "100"), doc("d2", "文档2", "ou_a", "100"), doc("d3", "文档3", "ou_a", "100")}
	api.walkErr["lib_of_other"] = true
	c := newUserConn(t, st, api, Config{WikiSpaceIDs: []string{"lib_of_other"}})
	round(t, c)

	api.folders["rootA"] = api.folders["rootA"][:2] // 云端删掉 d3
	got := round(t, c)
	if len(got) != 1 || got[0].ID != "del_d3_ou_a" {
		t.Fatalf("应产出 d3 的删除，实际 %v", kinds(got))
	}
}

// 第 4 条：团队库文档 A、B 都能看到，所有者 C（未授权）→ 三个人各一份。
func TestCopyTeamWikiEveryViewer(t *testing.T) {
	st := copyStore(t, "ou_a", "ou_b")
	api := newCopyAPI()
	api.wikiSpaces["ut0"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiSpaces["ut1"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiNodes["team"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_c"}}
	c := newUserConn(t, st, api, Config{})

	got := round(t, c)
	if len(got) != 1 {
		t.Fatalf("应产出 1 条写入，实际 %v", kinds(got))
	}
	if ids := got[0].OwnerIDs; len(ids) != 3 || ids[0] != "ou_c" || ids[1] != "ou_a" || ids[2] != "ou_b" {
		t.Fatalf("应写给 所有者 C、A、B，实际 %v", ids)
	}
	if p := copyPersons(t, st, "teamdoc"); len(p) != 3 {
		t.Fatalf("Ack 后应有 3 份副本，实际 %v", p)
	}
}

// 第 4 条：A 共享给未授权的 D → D 有一份；把 D 移出协作者 → 只删 D 的。
func TestCopyCollaboratorAddedAndRemoved(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "方案", "ou_a", "100")}
	api.members = map[string][]feishuapi.PermissionMember{"d1": {
		{MemberType: "openid", MemberID: "ou_a", Perm: "full_access"},
		{MemberType: "openid", MemberID: "ou_d", Perm: "view"},
		{MemberType: "openchat", MemberID: "oc_group", Perm: "view"},
	}}
	c := newUserConn(t, st, api, Config{CollabRefresh: time.Nanosecond}) // 每轮都刷新

	round(t, c)
	if p := copyPersons(t, st, "d1"); len(p) != 2 || p[0] != "ou_a" || p[1] != "ou_d" {
		t.Fatalf("应只有 A、D 两份（群不展开），实际 %v", p)
	}
	api.members["d1"] = api.members["d1"][:1] // 移除 D
	got := round(t, c)
	if len(got) != 1 || got[0].ID != "del_d1_ou_d" {
		t.Fatalf("应只删 D 的那份，实际 %v", kinds(got))
	}
	if p := copyPersons(t, st, "d1"); len(p) != 1 || p[0] != "ou_a" {
		t.Fatalf("应只剩 A 的副本，实际 %v", p)
	}
}

// 第 4 条：所有者在云端删掉文档 → 所有人的副本都删。
func TestCopyOwnerDeletesDoc(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "方案", "ou_a", "100")}
	api.members = map[string][]feishuapi.PermissionMember{"d1": {{MemberType: "openid", MemberID: "ou_d", Perm: "view"}}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.folders["rootA"] = nil
	got := round(t, c)
	if k := kinds(got); len(k) != 2 || k[0] != "delete:del_d1_ou_a" || k[1] != "delete:del_d1_ou_d" {
		t.Fatalf("应删除 A、D 两份，实际 %v", k)
	}
	if p := copyPersons(t, st, "d1"); len(p) != 0 {
		t.Fatalf("副本应全部清除，实际 %v", p)
	}
}

// 第 4 条：B 退出团队库 → 只删 B 的；A 仍能看到，C（所有者）、A 的不动。
func TestCopyViewerLosesAccess(t *testing.T) {
	st := copyStore(t, "ou_a", "ou_b")
	api := newCopyAPI()
	api.wikiSpaces["ut0"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiSpaces["ut1"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiNodes["team"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_c"}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.wikiSpaces["ut1"] = nil
	got := round(t, c)
	if len(got) != 1 || got[0].ID != "del_teamdoc_ou_b" {
		t.Fatalf("应只删 B 的那份，实际 %v", kinds(got))
	}
}

// 第 7 条：所有者 C 未授权，最后一个能看到它的授权员工也看不到了 → 剩下的副本一并删除。
func TestCopyNobodySeesUnauthorizedOwnerDoc(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.wikiSpaces["ut0"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiNodes["team"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_c"}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.wikiNodes["team"] = nil
	got := round(t, c)
	if k := kinds(got); len(k) != 2 || k[0] != "delete:del_teamdoc_ou_a" || k[1] != "delete:del_teamdoc_ou_c" {
		t.Fatalf("应删除 A、C 两份，实际 %v", k)
	}
}

// 熔断：B 本轮扫描出错 → B 的副本一份都不删。
func TestCopyScanFailureNoDelete(t *testing.T) {
	st := copyStore(t, "ou_b")
	api := newCopyAPI()
	api.wikiSpaces["ut0"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiNodes["team"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_c"}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.walkErr["team"] = true
	if got := round(t, c); len(got) != 0 {
		t.Fatalf("扫描出错时不应删除，实际 %v", kinds(got))
	}
}

// 第 5 条：我的文档库接口失败 → 算扫描失败，不删文档库里的文档。
func TestCopyMyLibraryErrorNoDelete(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "文档1", "ou_a", "100"), doc("d2", "文档2", "ou_a", "100"), doc("d3", "文档3", "ou_a", "100")}
	api.myLib["ut0"] = "mylib"
	api.wikiNodes["mylib"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "wikidoc1", ObjType: "docx", Title: "库文档", ObjEditTime: "100", Owner: "ou_a"}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.myLibErr = errors.New("rate limited")
	if got := round(t, c); len(got) != 0 {
		t.Fatalf("我的文档库接口失败时不应删除，实际 %v", kinds(got))
	}
}

// 未 Ack 的删除（Sink 删除失败）下一轮会重新产出。
func TestCopyDeleteNotAckedRetries(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "文档1", "ou_a", "100")}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.folders["rootA"] = nil
	recs, _, _ := c.Fetch(context.Background(), "", 100) // 产出删除但不 Ack
	if len(recs) != 1 || recs[0].Kind != "delete" {
		t.Fatalf("应产出删除，实际 %v", kinds(recs))
	}
	if got := round(t, c); len(got) != 1 || got[0].ID != "del_d1_ou_a" {
		t.Fatalf("未 Ack 的删除应重新产出，实际 %v", kinds(got))
	}
}

// 所有者 A 的文档放在 B 的文件夹里（A 的云盘树下列不到，只有 B 扫得到）→ 不能反复整篇删除再重下。
func TestCopyOwnerDocInOthersFolderNoFlapping(t *testing.T) {
	st := copyStore(t, "ou_a", "ou_b")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.roots["ut1"] = "rootB"
	api.folders["rootB"] = []feishuapi.DriveFile{doc("dInB", "A写在B文件夹的文档", "ou_a", "100")}
	c := newUserConn(t, st, api, Config{})

	round(t, c)
	if p := copyPersons(t, st, "dInB"); len(p) != 2 {
		t.Fatalf("应有 A、B 两份，实际 %v", p)
	}
	for i := 2; i <= 3; i++ {
		if got := round(t, c); len(got) != 0 {
			t.Fatalf("第 %d 轮不应产出任何记录，实际 %v", i, kinds(got))
		}
	}
}

// 整篇删除前，持有副本的人本轮必须都扫描成功；否则先不删。
func TestCopyDocDeleteBlockedByFailedHolder(t *testing.T) {
	st := copyStore(t, "ou_a", "ou_b")
	api := newCopyAPI()
	api.wikiSpaces["ut0"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiSpaces["ut1"] = []feishuapi.WikiSpace{{SpaceID: "team2"}}
	api.wikiNodes["team"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_c"}}
	api.wikiNodes["team2"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_c"}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.wikiNodes["team"] = nil  // A 看不到了
	api.walkErr["team2"] = true // B 这一轮扫描失败
	if got := round(t, c); len(got) != 0 {
		t.Fatalf("B 没扫成功时不应整篇删除，实际 %v", kinds(got))
	}
	api.walkErr["team2"] = false
	api.wikiNodes["team2"] = nil // B 也看不到了
	got := round(t, c)
	if k := kinds(got); len(k) != 3 {
		t.Fatalf("应删除 A、B、C 三份，实际 %v", k)
	}
}

// 第 1 条：妙记接口这一轮失败 → 不判断妙记删除，也不影响其他文档。
func TestCopyMinutesErrorNoDelete(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("d1", "文档1", "ou_a", "100")}
	api.minutes = []feishuapi.MinuteItem{{MinuteToken: "min1", Title: "周会", OwnerID: "ou_a", CreateTime: 1700000000000}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	api.minutes, api.minErr = nil, errors.New("rate limited")
	if got := round(t, c); len(got) != 0 {
		t.Fatalf("妙记接口失败时不应删除，实际 %v", kinds(got))
	}
	api.folders["rootA"] = nil // 其他文档的删除不受影响
	if got := round(t, c); len(got) != 1 || got[0].ID != "del_d1_ou_a" {
		t.Fatalf("云盘文档删除应照常产出，实际 %v", kinds(got))
	}
}

// 第 8 条：同一封邮件里两个同名附件 → 文件名不同。
func TestCopyMailSameNameAttachments(t *testing.T) {
	st := copyStore(t, "ou_a")
	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.mailIDs = []string{"msg_abcdefgh123"}
	api.mailMap["msg_abcdefgh123"] = &feishuapi.MailMessage{
		MessageID:   "msg_abcdefgh123",
		CreatedTime: 1700000000000,
		Attachments: []feishuapi.MailAttachment{{ID: "a1", Name: "合同.pdf"}, {ID: "a2", Name: "合同.pdf"}},
	}
	c := newUserConn(t, st, api, Config{})
	got := round(t, c)
	if len(got) != 1 || len(got[0].Attachments) != 2 {
		t.Fatalf("应产出 1 封邮件 2 个附件，实际 %+v", got)
	}
	if a, b := got[0].Attachments[0].Name, got[0].Attachments[1].Name; a != "合同_defgh123.pdf" || b != "合同_defgh123_2.pdf" {
		t.Fatalf("同名附件应区分，实际 %q %q", a, b)
	}
}

// 升级：旧版 feishu_doc_state 回填成所有者副本 → 升级前已同步、升级后被删的文档也会被删除。
func TestCopyBackfillFromDocState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "t.db")
	old, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	old.UpsertOAuthGrant(&storage.OAuthGrant{
		Platform: "feishu", ExternalUserID: "ou_a", AccessToken: "ut0", RefreshToken: "rt",
		ExpireAt: time.Now().Unix() + 3600, RefreshExpireAt: time.Now().Unix() + 86400, Status: "authorized",
	})
	old.UpsertDocState("dKeep", "保留_dKeep.docx", "ou_a", 100)
	old.UpsertDocState("dGone", "已删_dGone.docx", "ou_a", 100)
	old.Close()

	st, err := storage.Open(dbPath) // 重新打开时执行回填
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if p := copyPersons(t, st, "dGone"); len(p) != 1 || p[0] != "ou_a" {
		t.Fatalf("应回填所有者副本，实际 %v", p)
	}

	api := newCopyAPI()
	api.roots["ut0"] = "rootA"
	api.folders["rootA"] = []feishuapi.DriveFile{doc("dKeep", "保留", "ou_a", "100")}
	c := newUserConn(t, st, api, Config{})
	if got := round(t, c); len(got) != 1 || got[0].ID != "del_dGone_ou_a" {
		t.Fatalf("应只删除升级前已同步、现已不存在的 dGone，实际 %v", kinds(got))
	}
}

// 授权失效的所有者 A：B 失去权限后本轮谁都看不到 → 只删 B 的；A 的先留着，A 重新授权后再按正常规则判断。
func TestCopyInactiveOwnerKeptUntilReauth(t *testing.T) {
	st := copyStore(t, "ou_a", "ou_b")
	api := newCopyAPI()
	api.wikiSpaces["ut0"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiSpaces["ut1"] = []feishuapi.WikiSpace{{SpaceID: "team"}}
	api.wikiNodes["team"] = []feishuapi.WikiNode{{Token: "n1", ObjToken: "teamdoc", ObjType: "docx", Title: "团队文档", ObjEditTime: "100", Owner: "ou_a"}}
	c := newUserConn(t, st, api, Config{})
	round(t, c)

	setStatus := func(status string) {
		g, err := st.GetOAuthGrant("feishu", "ou_a")
		if err != nil {
			t.Fatal(err)
		}
		g.Status = status
		if err := st.UpsertOAuthGrant(g); err != nil {
			t.Fatal(err)
		}
	}
	setStatus("expired")
	api.wikiSpaces["ut1"] = nil // B 退出团队库
	got := round(t, c)
	if len(got) != 1 || got[0].ID != "del_teamdoc_ou_b" {
		t.Fatalf("应只删 B 的那份，实际 %v", kinds(got))
	}
	if p := copyPersons(t, st, "teamdoc"); len(p) != 1 || p[0] != "ou_a" {
		t.Fatalf("A 的副本应保留，实际 %v", p)
	}

	// A 重新授权，文档其实已被删 → A 的那份按正常规则删除
	setStatus("authorized")
	api.wikiNodes["team"] = nil
	got = round(t, c)
	if len(got) != 1 || got[0].ID != "del_teamdoc_ou_a" {
		t.Fatalf("A 重新授权后应删 A 的那份，实际 %v", kinds(got))
	}
}
