GOPROXY=direct go get github.com/korlvs/event-logging/contracts/event@contracts/event/v0.7.0

GOPROXY=direct go get github.com/korlvs/event-logging/libs/go-outbox@libs/go-outbox/v0.12.0

git tag libs/go-outbox/v0.12.0
git push origin libs/go-outbox/v0.12.0

git tag contracts/event/v0.7.0
git push origin contracts/event/v0.7.0