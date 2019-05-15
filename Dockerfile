FROM golang

LABEL maintainer="Vlad Kampov <vladyslav.kampov@gmail.com>"
ENV GO111MODULE=on
ENV SHORTENER_DB;
ENV SHORTENER_DB_USER;
ENV SHORTENER_DB_PASSWORD;
ENV SHORTENER_DOMAIN_PORT;
ENV SHORTENER_DOMAIN_WEB_URL;

WORKDIR $GOPATH/src/github.com/vladkampov/url-shortener
ADD . .

RUN go get -d -v ./...
RUN go install -v ./...
RUN go build main.go

EXPOSE 50051

CMD ["./main"]
