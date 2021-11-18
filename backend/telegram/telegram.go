package telegram

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
)

func init() {
	fsi := &fs.RegInfo{
		Name:        "telegram",
		Description: "Telegram Channel",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "bot_token",
			Help:     "Bot token for bot which is admin in channel",
			Required: true,
		}, {
			Name:     "channel_id",
			Help:     "ID for channel to store files in",
			Required: true,
		}},
	}
	fs.Register(fsi)
}

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	bot, err := tgbotapi.NewBotAPI(opt.BotToken)
	if err != nil {
		return nil, err
	}

	channelID := opt.ChannelID

	chat, err := bot.GetChat(tgbotapi.ChatInfoConfig{
		ChatConfig: tgbotapi.ChatConfig{
			ChatID: channelID,
		},
	})
	if err != nil {
		return nil, err
	}

	var fileIndexMessage *tgbotapi.Message
	if chat.PinnedMessage == nil {
		fileIndex := fileIndex{
			Files: make(map[string]fileIndexFile),
			mutex: &sync.Mutex{},
		}

		json, err := json.Marshal(fileIndex)
		if err != nil {
			return nil, err
		}

		file := tgbotapi.FileBytes{
			Name:  "index.json",
			Bytes: json,
		}

		msg, err := bot.Send(tgbotapi.NewDocument(channelID, file))
		if err != nil {
			return nil, err
		}

		fileIndexMessage = &msg

		_, err = bot.Request(tgbotapi.PinChatMessageConfig{
			ChatID:              channelID,
			MessageID:           msg.MessageID,
			DisableNotification: true,
		})
		if err != nil {
			return nil, err
		}
	} else {
		fileIndexMessage = chat.PinnedMessage
	}

	// download latest index
	url, err := bot.GetFileDirectURL(fileIndexMessage.Document.FileID)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// unmarshal index
	fileIndex := fileIndex{
		mutex: &sync.Mutex{},
	}
	err = json.NewDecoder(resp.Body).Decode(&fileIndex)
	if err != nil {
		return nil, err
	}

	// if len(root) > 0 && root[len(root)-1] != '/' {
	// 	root += "/"
	// } else if len(root) == 0 {
	// 	root = "/"
	// }

	root = filepath.Clean(root)

	ci := fs.GetConfig(ctx)
	f := &Fs{
		name:             name,
		root:             root,
		opt:              *opt,
		ci:               ci,
		bot:              bot,
		fileIndexMessage: fileIndexMessage,
		index:            &fileIndex,
		channelID:        channelID,
	}
	f.features = (&fs.Features{}).Fill(ctx, f)

	return f, nil
}

type fileIndexFile struct {
	FileID    string `json:"file_id"`
	MessageID int    `json:"message_id"`
	Size      int64  `json:"size"`
	ModTime   int64  `json:"mod_time"`
}

type fileIndex struct {
	Files map[string]fileIndexFile `json:"files"`
	mutex *sync.Mutex
}

func (fi *fileIndex) filesInDirectory(dir string) (map[string]fileIndexFile, map[string]struct{}) {
	files := make(map[string]fileIndexFile)
	directories := make(map[string]struct{})

	for name, file := range fi.Files {
		fileDir := filepath.Dir(name)
		if fileDir == dir {
			files[name] = file
			continue
		}

		if dir == "." || strings.Index(fileDir, dir) == 0 {
			trimmedPath := strings.TrimPrefix(name, dir)
			if strings.Contains(trimmedPath, "/") {
				directory := strings.Split(trimmedPath, "/")[0]
				if _, ok := directories[directory]; !ok && directory != "" {
					directories[directory] = struct{}{}
				}
			}
		}
	}

	return files, directories
}

func (fi *fileIndex) update(key string, fileIndexFile fileIndexFile) {
	fi.mutex.Lock()
	defer fi.mutex.Unlock()
	fi.Files[key] = fileIndexFile
}

func (fi *fileIndex) remove(key string) {
	fi.mutex.Lock()
	defer fi.mutex.Unlock()
	delete(fi.Files, key)
}

// Options defines the configuration for this backend
type Options struct {
	BotToken  string `config:"bot_token"`
	ChannelID int64  `config:"channel_id"`
}

type Fs struct {
	name             string
	root             string
	features         *fs.Features     // optional features
	opt              Options          // options for this backend
	ci               *fs.ConfigInfo   // global config
	bot              *tgbotapi.BotAPI // bot api
	fileIndexMessage *tgbotapi.Message
	index            *fileIndex
	channelID        int64 // channel id
	mutex            sync.Mutex
}

type Object struct {
	fs          *Fs
	remote      string
	key         string
	size        int64
	modTime     time.Time
	contentType string
}

func (f *Fs) updateIndex() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	json, err := json.Marshal(f.index)
	if err != nil {
		return err
	}

	file := tgbotapi.FileBytes{
		Name:  "index.json",
		Bytes: json,
	}

	_, err = f.bot.Send(tgbotapi.EditMessageMediaConfig{
		BaseEdit: tgbotapi.BaseEdit{
			ChatID:    f.channelID,
			MessageID: f.fileIndexMessage.MessageID,
		},
		Media: tgbotapi.NewInputMediaDocument(file),
	})
	if err != nil {
		return err
	}

	return nil
}

func (f *Fs) Name() string {
	return f.name
}

func (f *Fs) Root() string {
	return f.root
}

func (f *Fs) String() string {
	return f.name
}

func (f *Fs) Precision() time.Duration {
	return time.Second
}

func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

func (f *Fs) Features() *fs.Features {
	return f.features
}

func (f *Fs) objectFromFile(name string, fil fileIndexFile) *Object {
	// get name without root
	// remote := filepath.Base(name)
	return &Object{
		fs:          f,
		remote:      name,
		key:         name,
		size:        fil.Size,
		modTime:     time.Unix(fil.ModTime, 0),
		contentType: "",
	}
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	files, directories := f.index.filesInDirectory(filepath.Join(f.root, dir))

	for directory := range directories {
		entries = append(entries, fs.NewDir(directory, time.Now()))
	}

	for name, file := range files {
		entries = append(entries, f.objectFromFile(name, file))
	}

	return
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if val, ok := f.index.Files[remote]; ok {
		return f.objectFromFile(remote, val), nil
	} else {
		return nil, fs.ErrorObjectNotFound
	}
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	now := time.Now()

	file := tgbotapi.FileReader{
		Name:   src.Remote() + ".file",
		Reader: in,
	}

	msg, err := f.bot.Send(tgbotapi.NewDocument(f.channelID, file))
	if err != nil {
		return nil, err
	}

	fInd := fileIndexFile{
		FileID:    msg.Document.FileID,
		MessageID: msg.MessageID,
		Size:      src.Size(),
		ModTime:   now.Unix(),
	}

	f.index.update(src.Remote(), fInd)

	o := f.objectFromFile(src.Remote(), fInd)
	return o, o.fs.updateIndex()
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return nil
}

func (o *Object) Fs() fs.Info {
	return o.fs
}

func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

func (o *Object) Remote() string {
	return o.remote
}

func (o *Object) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

func (o *Object) Size() int64 {
	return o.size
}

func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	fil := o.fs.index.Files[o.key]
	fil.ModTime = modTime.Unix()

	o.fs.index.update(o.key, fil)
	o.modTime = modTime
	return o.fs.updateIndex()
}

func (o *Object) Storable() bool {
	return true
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	indexData := o.fs.index.Files[o.key]
	url, err := o.fs.bot.GetFileDirectURL(indexData.FileID)
	if err != nil {
		return nil, err
	}

	res, err := http.Get(url)
	if err != nil {
		return nil, errors.Wrap(err, "Open failed")
	}

	return res.Body, nil
}

func (o *Object) Remove(ctx context.Context) error {
	indexData := o.fs.index.Files[o.key]
	_, err := o.fs.bot.Request(tgbotapi.DeleteMessageConfig{
		ChatID:    o.fs.channelID,
		MessageID: indexData.MessageID,
	})
	if err != nil {
		return err
	}

	o.fs.index.remove(o.key)
	return o.fs.updateIndex()
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	indexData := o.fs.index.Files[o.key]
	file := tgbotapi.FileReader{
		Name:   src.Remote() + ".file",
		Reader: in,
	}

	_, err := o.fs.bot.Send(tgbotapi.EditMessageMediaConfig{
		BaseEdit: tgbotapi.BaseEdit{
			ChatID:    o.fs.channelID,
			MessageID: indexData.MessageID,
		},
		Media: tgbotapi.NewInputMediaDocument(file),
	})
	if err != nil {
		return err
	}

	fil := o.fs.index.Files[o.key]
	fil.Size = src.Size()
	o.fs.index.update(o.key, fil)

	o.size = src.Size()
	return o.fs.updateIndex()
}

func (o *Object) MimeType(ctx context.Context) string {
	return o.contentType
}

var (
	_ fs.Fs     = &Fs{}
	_ fs.Object = &Object{}
)
