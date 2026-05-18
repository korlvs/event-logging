GOPROXY=direct go get github.com/korlvs/event-logging/contracts/event@contracts/event/v0.1.0

GOPROXY=direct go get github.com/korlvs/event-logging/libs/go-outbox@libs/go-outbox/v0.1.0

git tag libs/go-outbox/v0.3.0
git push origin libs/go-outbox/v0.3.0