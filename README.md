# chip-aligning-pipeline
Pipeline for aligning ChIP seq reads from SRA files to reference genome

# Installation

## Requirements: development environment

While the pipeline itself needs only Python 3 to run, its dependencies also require Python 2 and R development environment set up.

Python can be installed without TCL/TK support:

```
brew install python2 --without-tcl-tk
brew install python3 --without-tcl-tk
```

R can be installed without xorg

```
brew install R --without-xorg
```

## Requirements : Command line applications

This pipeline depends on a number of command-line applications.
Make sure they are all available before you start. The list is provided below with hints on installation.

### Archiving applications

`unzip` and `gzip` and `7z` have to be in the path.

`gzip` should come with your linux distribution

Last tested version for 7z is 16.02 from linuxbrew `p7zip`.

```
brew install p7zip
```

Last tested version for unzip is stable 6.0, linuxbrew package `unzip`

```
brew install unzip
```

### Alignment

`bowtie2`, `bowtie2_build` have to be in path.
Last version tested: bowtie2: stable 2.3.4.1 (bottled) "bowtie2" package in linuxbrew.

```
brew install bowtie2
```

### Common system utilities

`cat`, `sort` and `cut`

These should come with most unix distributions by default.

### Python2 utilities

There are two python packages that the software makes use of as command line applications: `CrossMap` and `MACS2`.
These packages are available only in python2, so make sure to install them that way.

`CrossMap.py`: last tested version 0.2.7
`macs2`: last tested version 2.1.1.20160309

To install, first make sure `lzo` library is installed (needed for `bx-python`):

```
brew install lzo
```

Then install pre-requisites to py2 environment. Do not forget to use `pip2`, not `pip` (which should default to py3). 

```
pip2 install numpy
pip2 install cython
pip2 install bx-python
```

Finally install the packages.

```
pip2 install CrossMap macs2
```

### Phantompeakqualtools

`run_spp`

See https://github.com/kundajelab/phantompeakqualtools
Copied from their readme here for convenience.

First install `libxml2` as it's needed for `RCurl` - a dependency of `Rsamtools`.

```
brew install libxml2
```

Then: 

```
git clone https://github.com/kundajelab/phantompeakqualtools.git
cd phantompeakqualtools
R
```

Then, inside R:

```
> install.packages("snow", repos="http://cran.us.r-project.org")
> install.packages("snowfall", repos="http://cran.us.r-project.org")
> install.packages("bitops", repos="http://cran.us.r-project.org")
> install.packages("caTools", repos="http://cran.us.r-project.org")
> source("http://bioconductor.org/biocLite.R")
> biocLite("Rsamtools")
> install.packages("./spp_1.14.tar.gz")
```

Now quit R and edit the `run_spp.R` file, add
```
#!/usr/bin/env Rscript
```
To the first line of the script.

Copy this file to some directory in your path, e.g. if you're using linuxbrew:
```
cp run_spp.R ~/.linuxbrew/bin/
```

Make this file executable:
```
chmod +x chmod +x ~/.linuxbrew/bin/run_spp.R
```
It is now safe to `phantompeakqualtools` directory
```
cd ..
rm -rf phantompeakqualtools
```

### Samtools

`samtools`.
Last tested version 1.7 from linuxbrew.

```
brew install samtools
```

### SRA toolkit

`fastq-dump`

Last tested version: `sratoolkit` 2.9.0

```
brew install sratoolkit
```

### HDF5 support among with repacking utilities

`ptrepack` and `h5repack` utilities are necessary.

`h5repack` comes with `hdf5` in linuxbrew (last tested version 1.10.1)

```
brew install hdf5
```

`ptrepack` should be installed via tables package in python (will be installed automatically).

### UCSC Kent tools suite

`twoBitToFa`, `bigWigToBedGraph` and `bedClip` utilities are necessary for the pipeline.
They are available from UCSC suite

Last tested version stable 358, HEAD (from linuxbrew)

```
brew install kent-tools
```

## Requirements: Python requirements and package installation

Before we start with the package, we need to set up the development environment.
We will use python 3 for this.

First let's obtain `virtualenv` and `virtualenvwrapper` so we can keep a clean environment for our work.

```
pip3 install virtualenv virtualenvwrapper
```

Configure it to use python3 and add it to `.bash_profile` so the settings persist:

```
export VIRTUALENVWRAPPER_PYTHON=$HOME/.linuxbrew/bin/python3
source $HOME/.linuxbrew/bin/virtualenvwrapper.sh
echo "export VIRTUALENVWRAPPER_PYTHON=$HOME/.linuxbrew/bin/python3" >> ~/.bash_profile
echo "source $HOME/.linuxbrew/bin/virtualenvwrapper.sh" >> ~/.bash_profile
```

Make a virtual environment for this package
```
mkvirtualenv chipalign
```

From now on you can always go to this environment via `workon chipalign`.
To leave it type `deactivate`.
Note how `pip` and `python` now have been changed to virtualenv-specific ones.

First install some prerequisites needed to build the package:
```
pip install numpy
pip install cython
```

Obtain the source code by cloning this repository, e.g.
```
git clone https://github.com/lukauskas/chip-aligning-pipeline.git
```

Now change directory inside the pipeline
```
cd chip-aligning-pipeline
```

And install the package as editable

```
pip install -e .
```

# Testing command line dependancies post installation

To test whether all command-line dependancies are installed and working run the verify module

```
python -m chipalign.command_line_applications.verify
```

The expected output should list the application versions (or at least the name) for each of the apps:

```
* Archiving
** unzip
UnZip 6.00 of 20 April 2009, by Debian. Original by Info-ZIP.
** gzip
gzip 1.3.12
** 7z
7-Zip [64] 16.02 : Copyright (c) 1999-2016 Igor Pavlov : 2016-05-21
p7zip Version 16.02 (locale=C,Utf16=off,HugeFiles=on,64 bits,32 CPUs x64)
* Alignment
/home/ife/saulius.lukauskas/.linuxbrew/bin/../Cellar/bowtie2/2.3.4.1/bin/bowtie2-align-s version 2.3.4.1
/home/ife/saulius.lukauskas/.linuxbrew/Cellar/bowtie2/2.3.4.1/bin/bowtie2-build-s version 2.3.4.1
* Common
sort (GNU coreutils) 8.12
cut (GNU coreutils) 8.12
cat (GNU coreutils) 8.12
* Py2 based apps
Program: CrossMap (v0.2.7)
macs2 2.1.1.20160309
* Phantompeakqualtools
Got error return code ErrorReturnCode_1('\n\n  RAN: /home/ife/saulius.lukauskas/.linuxbrew/bin/run_spp.R\n\n  STDOUT:\n\n\n  STDERR:\n',) when running sh command
Usage: Rscript run_spp.R <options>
* Samtools
samtools 1.7
* SRA toolkit
/home/ife/saulius.lukauskas/.linuxbrew/bin/fastq-dump : 2.9.0
* HDF5
h5repack: Version 1.10.1
* UCSC Kent Tools
Got error return code ErrorReturnCode_255('\n\n  RAN: /usr/bin/twoBitToFa\n\n  STDOUT:\n\n\n  STDERR:\n',) when running sh command
twoBitToFa - Convert all or part of .2bit file to fasta
Got error return code ErrorReturnCode_255('\n\n  RAN: /home/ife/saulius.lukauskas/.linuxbrew/bin/bigWigToBedGraph\n\n  STDOUT:\n\n\n  STDERR:\n',) when running sh command
bigWigToBedGraph - Convert from bigWig to bedGraph format.
Got error return code ErrorReturnCode_255('\n\n  RAN: /home/ife/saulius.lukauskas/.linuxbrew/bin/bedClip\n\n  STDOUT:\n\n\n  STDERR:\n',) when running sh command
bedClip - Remove lines from bed file that refer to off-chromosome locations.
```

Ignore the "Got error return code ErrorReturnCode_1" lines, these come from logging system since the software sometimes returns an error code for help screen"

# Working with the pipeline

The whole mapping pipeline is described as a set of [`luigi`](https://github.com/spotify/luigi) tasks.
Working with the pipeline follows the standard `luigi` working practices and therefore it is recommended to read its [documentation](https://luigi.readthedocs.io/en/stable/) prior to starting the examples here.

# `chipalign.yml`

The pipeline assumes existence of a configuration file `chipalign.yml` in the working directory of the script that is being run.
This file should contain one variable, in Yaml format, that points to the directory where the output should be stored.
For instance, see the [`chipalign.yml` in the examples directory](https://github.com/lukauskas/chip-aligning-pipeline/blob/master/examples/chipalign.yml) that directs the output of the program to `'output/'` directory. It is, however, a good advice to use absolute path of the output directory, instead of a relative one.

# Running pipeline on multiple processes

The package `luigi`, which the pipeline is based on, supports execution of the tasks in a parallel fashion.
By default, however, this option is turned off. You can, however, specify the number of workers by providing the `--workers` option in the command line, i.e. `--workers 6` will use 6 workers at the same time to execute the pipeline. Be aware though that the memory requirement grows with every worker that is being used.

# Automatic result invalidation

The pipeline is designed to automatically track of the file modification times.
Generally, a `luigi` task would be complete once its output file has been generated.
Tasks deriving from `chipalign.core.task.Task`, however, also have the constraint on the modification time of these imputs.
Therefore a given task is complete iff its output file exists, all of its children are complete, and the modification time of the output file for the task is greater than the modification times of the child task outputs. Furthermore, the task also checks whether its source code file has been modified since the last time output has been generated, and invalidates the output files, if it has.
