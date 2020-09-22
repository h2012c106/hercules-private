FROM docker-reg.devops.xiaohongshu.com/library/maven:3.6.0-jdk-8-alpine
WORKDIR /build
ADD ./.ssh /root/.ssh
RUN chmod 0600 /root/.ssh/id_rsa \
    && chmod 0600 /root/.ssh/id_rsa.pub \
    && sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories \
    && apk add --no-cache openssh \
    && mkdir ~/.pip

#ADD ./pip.conf /root/.pip
#
#RUN apk add --no-cache \
#        python3 \
#        py3-pip \
#    && pip3 install --upgrade pip \
#    && pip3 install \
#        awscli \
#    && rm -rf /var/cache/apk/* \
#    && aws configure set region cn-north-1 --profile middleware \

USER root