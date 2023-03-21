FROM golang:1.18.4-alpine3.16

RUN apk update && apk --no-cache add ca-certificates

RUN apk update && apk add tzdata
RUN cp /usr/share/zoneinfo/Asia/Kolkata /etc/localtime
RUN echo "Asia/Kolkata" > /etc/timezone

RUN apk update && apk add git
RUN mkdir -p /WorkflowManagement/Core/GeneralGoWorkflows/
ADD . /WorkflowManagement/Core/GeneralGoWorkflows/

ENV SERVICE=general-go-workflows
ENV NAMESPACE=workflows
ENV CONFIG_DIR=/WorkflowManagement/Core/GeneralGoWorkflows/config
ENV ENV=dev
WORKDIR /WorkflowManagement/Core/GeneralGoWorkflows

#RUN go get -v github.com/nurture-farm/Contracts@latest
#RUN go mod tidy
#RUN go mod vendor
RUN go build -o main .

EXPOSE 4000
EXPOSE 7000
CMD ["/WorkflowManagement/Core/GeneralGoWorkflows/main"]

