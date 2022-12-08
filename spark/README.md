# Pagerank Spark


Install the development environment

[sdkman](https://sdkman.io/install)

```bash
sdk install java 17.0.1
sdk install scala 2.13.10
sdk install sbt 1.7.3
```

Install & Configure Spark

```bash
curl -fsSLO "https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz"
tar xvf spark-3.3.1-bin-hadoop3-scala2.13.tgz
rm spark-3.3.1-bin-hadoop3-scala2.13.tgz
mv spark-3.3.1-bin-hadoop3-scala2.13 /usr/local/spark
export PATH="/usr/local/spark/bin/:$PATH"
```

Fat Package Scala as JAR

```bash
sbt assembly
```

Run Spark pageRank with 1 worker

Set the current directory

```bash
ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}/spark/pagerank"
spark-submit \
  --class "PageRank" \
  --deploy-mode "client" \
  --master "local[1]" \
  "${ROOT}/spark/pagerank/target/scala-2.13/pagerank-assembly-1.0.0.jar" \
  "${ROOT}/dataset/asset/1997-spark-verts.txt" \
  "${ROOT}/dataset/asset/1997-spark-edges.txt" \
  5
```

Run Spark pageRank with 1 worker for all years

```bash
for Y in {1992..2002};
do spark-submit \
  --class "PageRank" \
  --deploy-mode "client" \
  --master "local[1]" \
  "${ROOT}/spark/pagerank/target/scala-2.13/pagerank-assembly-1.0.0.jar" \
  "${ROOT}/dataset/asset/${Y}-spark-verts.txt" \
  "${ROOT}/dataset/asset/${Y}-spark-edges.txt" \
  5 >> log.txt;
done;
```


