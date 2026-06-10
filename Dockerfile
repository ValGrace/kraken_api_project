FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY CDC.sh /app/CDC.sh
RUN chmod +x /app/CDC.sh

COPY . .

ENTRYPOINT [ "/app/CDC.sh" ]
CMD ["start"]

