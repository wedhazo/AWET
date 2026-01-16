FROM node:18-alpine

WORKDIR /app

# Clone SuperAGI GUI
RUN apk add --no-cache git && \
    git clone --depth 1 https://github.com/TransformerOptimus/SuperAGI.git /tmp/superagi && \
    cp -r /tmp/superagi/gui/* /app/ && \
    rm -rf /tmp/superagi

# Install dependencies
RUN npm install

# Build argument for API URL
ARG NEXT_PUBLIC_API_BASE_URL="/api"
ENV NEXT_PUBLIC_API_BASE_URL=$NEXT_PUBLIC_API_BASE_URL

# Build the app
RUN npm run build

EXPOSE 3000

CMD ["npm", "start"]
