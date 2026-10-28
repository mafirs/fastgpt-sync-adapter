// 飞书 Drive / Docx API：云文档遍历与内容下载。
// 调用方按身份传入 token（tenant_access_token 或 user_access_token），本文件不感知鉴权细节。
package feishu

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// DriveFile drive/v1/files 返回的文件条目。
type DriveFile struct {
	Token        string `json:"token"`
	Name         string `json:"name"`
	Type         string `json:"type"` // folder/doc/docx/sheet/bitable/file/slides/mindnote/shortcut
	ParentToken  string `json:"parent_token"`
	OwnerID      string `json:"owner_id"`
	CreatedTime  string `json:"created_time"`  // 秒级时间戳字符串
	ModifiedTime string `json:"modified_time"` // 秒级时间戳字符串
	URL          string `json:"url"`
}

// get 带鉴权的 GET（data 字段解到 out）。网络错误与限流类错误自动重试。
func (c *Client) get(path, token string, query url.Values, out any) error {
	u := baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var lastErr error
	for attempt := 0; attempt < apiAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
			log.Printf("[feishu] GET %s 重试 %d/%d（上次: %v）", path, attempt+1, apiAttempts, lastErr)
		}
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var ar apiResp
		if err := json.Unmarshal(body, &ar); err != nil {
			lastErr = fmt.Errorf("解析响应失败: %w, body: %s", err, truncate(string(body), 300))
			continue
		}
		if ar.Code != 0 {
			lastErr = fmt.Errorf("飞书错误 code=%d msg=%s", ar.Code, ar.Msg)
			if retryableCode(ar.Code) {
				continue
			}
			return lastErr
		}
		if out != nil {
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("解析响应数据失败: %w", err)
			}
		}
		return nil
	}
	return lastErr
}

// ListFolderChildren 列文件夹直接子项（自动翻页）。
func (c *Client) ListFolderChildren(token, folderToken string) ([]DriveFile, error) {
	var all []DriveFile
	pageToken := ""
	for {
		q := url.Values{}
		q.Set("page_size", "200")
		if folderToken != "" {
			q.Set("folder_token", folderToken)
		}
		if pageToken != "" {
			q.Set("page_token", pageToken)
		}
		var out struct {
			apiResp
			Data struct {
				Files     []DriveFile `json:"files"`
				HasMore   bool        `json:"has_more"`
				PageToken string      `json:"next_page_token"` // 该接口的翻页字段是 next_page_token，不是 page_token
			} `json:"data"`
		}
		if err := c.get("/drive/v1/files", token, q, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Data.Files...)
		if !out.Data.HasMore {
			return all, nil
		}
		// 还有下一页却没给翻页标记：报错而不是返回半截列表，否则没列到的文档会被当成已删除
		if out.Data.PageToken == "" {
			return nil, fmt.Errorf("列文件夹 %s：has_more=true 但没有 next_page_token", folderToken)
		}
		pageToken = out.Data.PageToken
	}
}

// WalkFolder 递归遍历文件夹（含子文件夹），回调每个非文件夹条目；深度封顶防失控。
func (c *Client) WalkFolder(token, folderToken string, depth int, fn func(DriveFile)) error {
	if depth <= 0 {
		return nil
	}
	children, err := c.ListFolderChildren(token, folderToken)
	if err != nil {
		return err
	}
	for _, f := range children {
		if f.Type == "folder" {
			if err := c.WalkFolder(token, f.Token, depth-1, fn); err != nil {
				return err
			}
			continue
		}
		if f.Type == "shortcut" {
			continue // 快捷方式不跟（防环）
		}
		fn(f)
	}
	return nil
}

// RootFolderMeta 当前身份的云空间根目录 token（user token 调）。
func (c *Client) RootFolderMeta(token string) (string, error) {
	var out struct {
		apiResp
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := c.get("/drive/explorer/v2/root_folder/meta", token, nil, &out); err != nil {
		return "", err
	}
	return out.Data.Token, nil
}

// DownloadFile 下载 drive 里上传类文件（type=file）的原始字节。
// 注意限流：drive 下载约 5 QPS、约 1 万次/天/应用，调用方要自控速。
func (c *Client) DownloadFile(token, fileToken string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", baseURL+"/drive/v1/files/"+fileToken+"/download", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("下载失败 http=%d: %s", resp.StatusCode, truncate(string(body), 300))
	}
	return resp.Body, nil
}

// DocxRawContent 新版文档（docx）纯文本内容。
func (c *Client) DocxRawContent(token, documentID string) (string, error) {
	var out struct {
		apiResp
		Data struct {
			Content string `json:"content"`
		} `json:"data"`
	}
	if err := c.get("/docx/v1/documents/"+documentID+"/raw_content", token, nil, &out); err != nil {
		return "", err
	}
	return out.Data.Content, nil
}

// DocRawContent 旧版文档（doc）纯文本内容。
func (c *Client) DocRawContent(token, docToken string) (string, error) {
	var out struct {
		apiResp
		Data struct {
			Content string `json:"content"`
		} `json:"data"`
	}
	if err := c.get("/doc/v2/"+docToken+"/raw_content", token, nil, &out); err != nil {
		return "", err
	}
	return out.Data.Content, nil
}

// UnixSeconds 解析飞书的秒级时间戳字符串（失败返回 0）。
func UnixSeconds(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

// NowUnix 当前秒级时间戳。
func NowUnix() int64 { return time.Now().Unix() }

// SearchDocEntity 搜索返回的云文档条目。
type SearchDocEntity struct {
	Token     string `json:"docs_token"`
	Type      string `json:"docs_type"`
	Title     string `json:"title"`
	OwnerID   string `json:"owner_id"`
	OwnerName string `json:"owner_name"`
}

// SearchDocs 搜索用户有权访问的云文档（包含“与我共享”的散落文件）。
func (c *Client) SearchDocs(userToken, searchKey string, offset, count int) ([]SearchDocEntity, error) {
	body := map[string]any{
		"search_key": searchKey,
		"offset":     offset,
		"count":      count,
	}
	var out struct {
		apiResp
		Data struct {
			DocsEntities []SearchDocEntity `json:"docs_entities"`
			Total        int               `json:"total"`
		} `json:"data"`
	}
	if err := c.postJSON(baseURL+"/suite/docs-api/search/object", body, userToken, &out); err != nil {
		return nil, err
	}
	return out.Data.DocsEntities, nil
}

// PermissionMember 文档协作者。
type PermissionMember struct {
	MemberType string `json:"member_type"` // openid / openchat / opendepartmentid / ...
	MemberID   string `json:"member_id"`
	Perm       string `json:"perm"` // view / edit / full_access
}

// ListPermissionMembers 列出文档协作者（GET /drive/v1/permissions/{token}/members）。
// docType 取 doc/docx/sheet/bitable/file/slides 等；调用者必须是该文档的协作者（所有者可以）。
func (c *Client) ListPermissionMembers(userToken, token, docType string) ([]PermissionMember, error) {
	q := url.Values{}
	q.Set("type", docType)
	var out struct {
		apiResp
		Data struct {
			Items []PermissionMember `json:"items"`
		} `json:"data"`
	}
	if err := c.get("/drive/v1/permissions/"+token+"/members", userToken, q, &out); err != nil {
		return nil, err
	}
	return out.Data.Items, nil
}
