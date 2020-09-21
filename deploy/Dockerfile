FROM docker-reg.devops.xiaohongshu.com/library/maven:3.6.0-jdk-8-alpine
WORKDIR /build
ADD ./.ssh /root/.ssh
RUN chmod 0600 /root/.ssh/id_rsa \
    && chmod 0600 /root/.ssh/id_rsa.pub \
    && sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories \
    && apk add --no-cache openssh
USER root
ENV ANSIBLE_HOST_KEY_CHECKING=False
