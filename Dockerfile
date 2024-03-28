FROM python:alpine3.19

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x start_parser.sh

CMD ["sh", "./start_parser.sh"]
