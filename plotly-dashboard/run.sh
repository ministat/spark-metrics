docker run --name spark-metrics -p 8000:8088 -v `pwd`/data:/usr/src/app/data -it hongjiang/spark-metrics-monitor
