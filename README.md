# video-recommendation

##Semantic Video Recommendation Engine
VideoRecommendation.snb is scala spark notebook that needs spark notbook hosted at http://spark-notebook.io

###Steps for executing code in scala-spark notebook
####Download Notebook hosted at following link
https://s3.eu-central-1.amazonaws.com/spark-notebook/zip/spark-notebook-0.6.3-scala-2.11.7-spark-1.6.0-hadoop-2.2.0-with-hive-with-parquet.zip?max-keys=100000

#### Unzipping download zip file
unzip spark-notebook-0.6.3-scala-2.11.7-spark-1.6.0-hadoop-2.2.0-with-hive-with-parquet.zip -d spark-notebook-0.6.3-scala-2.11.7-spark-1.6.0-hadoop-2.2.0-with-hive-with-parquet

#### Change directory to notebook directory
cd /path/spark-notebook-0.6.3-scala-2.11.7-spark-1.6.0-hadoop-2.2.0-with-hive-with-parquet

#### Run Notebook server locally
./bin/spark-notebook

#### Open spark notebook page in browser
open localhost:9000 in the browser

#### Import the notebook in the repo
import and upload VideoRecommendation.snb notebook

#### CLick on the notebook link to open it
click on VideoRecommendation.snb to open notebook

#### Set inputPath variable to path where data exist i.e /path/video-recommendation/videoMetaDataInCsvFormat
#### Use shift-enter to execute each shell

### Steps for executing code in python spark notebook
#### Options for opening python notebook with pyspark shell
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' --NotebookApp.port=8880"

#### Open pyspark shell in spark binary
./bin/pyspark

### Open python notebook server in browser
https://localhost:8880






