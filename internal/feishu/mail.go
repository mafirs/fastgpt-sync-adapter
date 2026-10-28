package feishu

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// MailMessage 邮件信息。
type MailMessage struct {
	MessageID   string           `json:"message_id"`
	Subject     string           `json:"subject"`
	HeadFrom    string           `json:"head_from"`
	CreatedTime int64            `json:"created_time"` // 毫秒
	Attachments []MailAttachment `json:"attachments"`
}

// MailAttachment 邮件附件元数据。
type MailAttachment struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	IsInline bool   `json:"is_inline"`
}

// ListMailboxMessages 列出用户收件箱邮件 ID 列表（每次最多 20 封；用户未开通飞书邮箱时自动平滑跳过）。
func (c *Client) ListMailboxMessages(userToken string, pageSize int) ([]string, error) {
	if pageSize <= 0 || pageSize > 20 {
		pageSize = 20
	}
	u := fmt.Sprintf("%s/mail/v1/user_mailboxes/me/messages?page_size=%d", baseURL, pageSize)
	var out struct {
		apiResp
		Data struct {
			HasMore   bool            `json:"has_more"`
			PageToken string          `json:"page_token"`
			Items     json.RawMessage `json:"items"`
		} `json:"data"`
	}
	if err := c.getWithToken(u, userToken, &out); err != nil {
		errStr := err.Error()
		// 15180001 / 4013：用户未开通企业邮箱，静默返回空
		if strings.Contains(errStr, "15180001") || strings.Contains(errStr, "4013") {
			return nil, nil
		}
		return nil, err
	}
	if len(out.Data.Items) == 0 {
		return nil, nil
	}
	// 兼容支持 []string 数组格式
	var strIDs []string
	if err := json.Unmarshal(out.Data.Items, &strIDs); err == nil {
		return strIDs, nil
	}
	// 兼容支持 []struct{ ID string `json:"id"` } 对象数组格式
	var objIDs []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(out.Data.Items, &objIDs); err == nil {
		var ids []string
		for _, o := range objIDs {
			if o.ID != "" {
				ids = append(ids, o.ID)
			}
		}
		return ids, nil
	}
	return nil, fmt.Errorf("解析邮件列表失败: 无法识别 items 格式")
}

// GetMailMessage 获取某封邮件的详情与附件清单。
func (c *Client) GetMailMessage(userToken, messageID string) (*MailMessage, error) {
	u := fmt.Sprintf("%s/mail/v1/user_mailboxes/me/messages/%s", baseURL, messageID)
	var out struct {
		apiResp
		Data struct {
			Message struct {
				MessageID    string `json:"message_id"`
				Subject      string `json:"subject"`
				HeadFrom     struct {
					MailAddress string `json:"mail_address"`
					Name        string `json:"name"`
				} `json:"head_from"`
				InternalDate string `json:"internal_date"`
				Attachments  []struct {
					ID             string `json:"id"`
					Filename       string `json:"filename"`
					Size           int64  `json:"size"`
					AttachmentType int    `json:"attachment_type"`
					IsInline       bool   `json:"is_inline"`
				} `json:"attachments"`
			} `json:"message"`
		} `json:"data"`
	}
	if err := c.getWithToken(u, userToken, &out); err != nil {
		return nil, err
	}
	m := out.Data.Message
	ts, _ := strconv.ParseInt(m.InternalDate, 10, 64)
	res := &MailMessage{
		MessageID:   m.MessageID,
		Subject:     m.Subject,
		HeadFrom:    m.HeadFrom.MailAddress,
		CreatedTime: ts,
	}
	for _, a := range m.Attachments {
		res.Attachments = append(res.Attachments, MailAttachment{
			ID:       a.ID,
			Name:     a.Filename,
			Size:     a.Size,
			IsInline: a.IsInline,
		})
	}
	return res, nil
}

// DownloadMailAttachment 从附件专属下载链接拉取文件流。
func (c *Client) DownloadMailAttachment(userToken, messageID, attachmentID string) (io.ReadCloser, error) {
	q := url.Values{}
	q.Set("attachment_ids", attachmentID)
	u := fmt.Sprintf("%s/mail/v1/user_mailboxes/me/messages/%s/attachments/download_url?%s",
		baseURL, messageID, q.Encode())

	var out struct {
		apiResp
		Data struct {
			DownloadURLs []struct {
				AttachmentID string `json:"attachment_id"`
				DownloadURL  string `json:"download_url"`
			} `json:"download_urls"`
			FailedIDs     []string `json:"failed_ids"`
			FailedReasons []struct {
				AttachmentID string `json:"attachment_id"`
				Reason       string `json:"reason"`
			} `json:"failed_reasons"`
		} `json:"data"`
	}
	if err := c.getWithToken(u, userToken, &out); err != nil {
		return nil, fmt.Errorf("获取附件下载链接: %w", err)
	}

	for _, failedID := range out.Data.FailedIDs {
		if failedID == attachmentID {
			reason := ""
			for _, fr := range out.Data.FailedReasons {
				if fr.AttachmentID == attachmentID {
					reason = fr.Reason
					break
				}
			}
			return nil, fmt.Errorf("附件 %s 获取下载链接失败: %s", attachmentID, reason)
		}
	}

	dlURL := ""
	for _, item := range out.Data.DownloadURLs {
		if item.AttachmentID == attachmentID {
			dlURL = item.DownloadURL
			break
		}
	}
	if dlURL == "" {
		return nil, fmt.Errorf("未获取到附件 %s 的下载链接", attachmentID)
	}

	req, err := http.NewRequest("GET", dlURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("下载邮件附件失败 http=%d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	return resp.Body, nil
}
