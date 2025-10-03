# imagem base  Node
FROM node:18-alpine

# python p rodar scrips (rodei local)
RUN apk update && apk add --no-cache \
    python3 \
    py3-pip \
    && pip3 install --break-system-packages pandas mysql-connector-python


WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 3000


CMD ["npm", "start"]