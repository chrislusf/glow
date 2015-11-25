// mongodb_source shows how to read from external sources and cogroup()
package main

import (
	"flag"
	"fmt"
	"time"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
	_ "github.com/go-sql-driver/mysql"
	"labix.org/v2/mgo"
)

type Attachment struct {
	Name        string `bson:"name,omitempty", json:"name,omitempty"`
	ContentType string `bson:"ct,omitempty", json:"ct,omitempty"`
	ContentId   string `bson:"cid,omitempty", json:"cid,omitempty"`
	FileId      FileId `bson:"fid,omitempty", json:"fid,omitempty"`
	Size        int    `bson:"size,omitempty", json:"size,omitempty"`
}

type FileId string

type PostId string

type Post struct {
	Id           PostId       `bson:"_id,omitempty", json:"id,omitempty"`
	Email        string       `bson:"email,omitempty", json:"email,omitempty"`
	Subject      string       `bson:"subject,omitempty", json:"subject,omitempty"`
	CreationTime time.Time    `bson:"createdTime,omitempty", json:"createdTime,omitempty"`
	Html         FileId       `bson:"html,omitempty", json:"html,omitempty"`
	Attachments  []Attachment `bson:"attachments,omitempty", json:"attachments,omitempty"`
	Inlines      []Attachment `bson:"inlines,omitempty", json:"inlines,omitempty"`
	UpdateTime   time.Time    `bson:"updatedTime,omitempty", json:"updatedTime,omitempty"`
}

type User struct {
	Name         string    `bson:"name,omitempty", json:"name,omitempty"`
	Email        string    `bson:"email,omitempty", json:"email,omitempty"`
	CreationTime time.Time `bson:"createdTime,omitempty", json:"createdTime,omitempty"`
	LastSeenTime time.Time `bson:"lastSeenTime,omitempty", json:"lastSeenTime,omitempty"`
}

type UserPosts struct {
	user  User
	posts []Post
}

var (
	f1 *flow.FlowContext
)

func iterate(mongodbUrl, dbName, collectionName string, fn func(*mgo.Iter)) {
	session, err := mgo.Dial(mongodbUrl)
	if err != nil {
		println(err)
		return
	}
	iter := session.DB(dbName).C(collectionName).Find(nil).Iter()
	fn(iter)
	if err := iter.Close(); err != nil {
		println(err)
	}
}

func init() {
	f1 = flow.New()
	users := f1.Source(func(out chan User) {
		iterate("mongodb://127.0.0.1", "example", "users", func(iter *mgo.Iter) {
			var user User
			for iter.Next(&user) {
				out <- user
			}
		})
	}, 1).Map(func(user User) (string, User) {
		return user.Email, user
	}).Partition(3)

	posts := f1.Source(func(out chan Post) {
		iterate("mongodb://127.0.0.1", "example", "posts", func(iter *mgo.Iter) {
			var post Post
			for iter.Next(&post) {
				out <- post
			}
		})
	}, 1).Map(func(post Post) (string, Post) {
		return post.Email, post
	}).Partition(3)

	users.CoGroup(posts).Map(func(email string, users []User, posts []Post) (User, []Post) {
		if len(users) > 0 {
			return users[0], posts
		} else {
			return User{}, posts
		}
	}).Filter(func(user User, posts []Post) bool {
		return user.LastSeenTime.Before(time.Now().AddDate(-1, 0, 0))
	}).Map(func(user User, posts []Post) {
		var totalSize int
		for _, post := range posts {
			for _, att := range post.Attachments {
				totalSize += att.Size
			}
			for _, att := range post.Inlines {
				totalSize += att.Size
			}
		}
		if len(posts) > 0 {
			fmt.Print(posts[0].Id, " ", posts[0].Email)
		}
		fmt.Println(" user", user.Email, "posts", len(posts), "bytes", totalSize, "lastSeen", user.LastSeenTime)
	})

}

func main() {
	flag.Parse()

	flow.Ready()

	f1.Run()

}
