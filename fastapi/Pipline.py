from helpers.Streaming import start_streaming
import os
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
HADOOP_PATH = os.path.join(PROJECT_DIR, "hadoop")
os.environ["PYSPARK_PYTHON"] = r"C:\Users\aamir\AppData\Local\Programs\Python\Python313\python.exe"

os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\aamir\AppData\Local\Programs\Python\Python313\python.exe"


os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["hadoop.home.dir"] = HADOOP_PATH
os.environ["PATH"] = os.path.join(HADOOP_PATH, "bin") + ";" + os.environ["PATH"]

print("HADOOP_HOME =", os.environ["HADOOP_HOME"])
print("PATH =", os.environ["PATH"])
if __name__ == "__main__":
    start_streaming()
