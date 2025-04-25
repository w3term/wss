FROM node:22.15-alpine

ENV NODE_ENV=production

WORKDIR /usr/src/app

# Copy package files first for better caching
COPY package*.json ./
RUN npm ci --omit=dev

# Copy the rest of the source files into the image.
COPY . .

# Run the application as a non-root user.
USER node

# Indicate the port that the application listens on.
EXPOSE 8081

# Run the application.
CMD ["node", "main.js"]
