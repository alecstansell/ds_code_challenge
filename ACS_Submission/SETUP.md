# Setting up a PySpark Development Environment

This project requires a PySpark environment to run. While the code would typically be run on a Spark cluster, it can be useful to develop and test the code using a local environment setup. This can later be deployed to an environment with a Spark cluster. Details of a potential EC2 or Databricks deployment are included below.


# Requirements

To set up a local PySpark environment, you will need to have the following installed:

- Java 8
- Python 3.x
- Apache Spark (including PySpark)
- Jupyter Notebook

# Example Installation

One way to set up a PySpark development environment with Jupyter is to use Homebrew. Here is an example installation process:

## Install Homebrew (if you haven't already):

``` 
bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

```

## Set brew to path 

```
bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> /Users/admin/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

## Install Java  (OpenJDK 11)

You can install the java development kit through:

```
bash
brew install openjdk@11
```

## Install python

``` 
bash
brew install python

```

## Install apache spark (including PySpark):

```
bash
brew install apache-spark

```

## Use Conda to Manage Further Dependencies

To manage additional dependencies for this project you can create a conda environment.

### Conda installation 

Details available here: https://www.anaconda.com/products/distribution#Downloads

## Installation of conda with ipykernel to manage envs 

### Install jupyter notebook and conda kernels 

```
bash
conda install -c conda-forge notebook
conda install -c conda-forge nb_conda_kernels

```
### Install jupyter labs
```
bash
conda install -c conda-forge jupyterlab
conda install -c conda-forge nb_conda_kernels

```

## Create conda environment

```
bash
conda create -n ctds ipykernel python=3.9
```

## Install requirements 

```
bash
conda install --file requirements.txt
```

# Running PySpark in Command Line

To open a Jupyter notebook, run
```
bash
pyspark
```
or 

```
bash
jupyter notebook
```

# Open a Notebook in Your Environment

To open a new notebook in your specified environment, go to the "New" dropdown and select "Notebook: Python [pyspark_env]". You can change the kernel through the "Kernel" dropdown and selecting "Change kernel" > "ctds".

## Deployment Options

### Databricks

Databricks is a managed Spark cluster platform that allows you to easily deploy and manage your Spark applications. To deploy this code on Databricks, you can follow these steps:

# Create a Databricks workspace if you haven't already.

Create a new cluster in your workspace with the appropriate Spark version and configuration.
Upload the code to the workspace.
Create a new notebook in the workspace and configure it to use the uploaded code.
Run the notebook to execute the code on the cluster.
For more detailed instructions, you can refer to the Databricks documentation.

# Amazon EC2

Amazon EC2 is a popular cloud computing service that allows you to create virtual machines (instances) and run your applications on them. To deploy this code on an EC2 instance, you can follow these steps:

# Launch an EC2 instance with the appropriate configuration (e.g., Ubuntu, Java, Spark).

Copy the code to the instance using scp or another file transfer method.
Install any necessary dependencies (e.g., Python packages) on the instance.
Start a Spark session using pyspark or another method.
Run the code on the Spark session.
For more detailed instructions, you can refer to the EC2 documentation.

# Resources
Two useful articles for the above.

## Jupyter conda setup

https://towardsdatascience.com/how-to-set-up-anaconda-and-jupyter-notebook-the-right-way-de3b7623ea4a#:~:text=Attention%3A%20You%20always%20need%20to,notebook%20in%20the%20base%20environment.


## Spark  mac brew setup


https://sparkbyexamples.com/pyspark/install-pyspark-in-jupyter-on-mac-using-homebrew/