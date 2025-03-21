FROM nginx:latest

# Remove default nginx configuration
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom configuration
COPY ./nginx.conf /etc/nginx/conf.d/

# Expose port 80
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]