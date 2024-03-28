FROM python:3.12

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x start_bot.sh

CMD ["./start_bot.sh"]
