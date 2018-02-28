# chip-aligning-pipeline
Pipeline for aligning ChIP seq reads from SRA files to reference genome

# Installation

## Requirements: development environment

While the pipeline itself needs only Python 3 to run, its dependencies also require Python 2 and R develoipment environment set up.

## Requirements : Command line applications

This pipeline depends on a number of command-line applications.
Make sure they are all available before you start. The list is provided below with hints on installation.

### Archiving applications

`unzip` and `gzip` and `7z` have to be in the path.

Last tested version for 7z: 
16.02 from linuxbrew `p7zip`.

### Alignment

`bowtie2`, `bowtie2_build` have to be in path.
Last version tested: bowtie2: stable 2.3.4.1 (bottled) "bowtie2" package in linuxbrew.

Also cmd `pash3` has to be in path.
TODO: Remove pash3

### Common system utilities (in most linux systems)

`cat`, `sort` and `cut`

### CrossMap

`CrossMap.py`.
Can be installed via pip: `pip install CrossMap`
Last version tested: 0.2.7

### Peak Calling

`macs2`. 
Can be installed via pip `pip install macs2`.
Note this needs Python 2.7, not python 3.
Last version tested: 2.1.1.20160309

### Phantompeakqualtools

`run_spp`

See https://github.com/kundajelab/phantompeakqualtools

### Samtools

`samtools`.
Last tested version 1.7 from linuxbrew.

### SRA toolkit

`fastq-dump`

Last tested version: TODO - find on linuxbrew

### Tables
`ptrepack` and `h5repack` utilities are necessary.

TODO: how to install

### UCSC suite

`twoBitToFa`, `bigWigToBedGraph` and `bedClip` utilities are necessary for the pipeline.
They are available from UCSC suite

TODO: how to install

## Requirements: Python requirements and package installation

Obtain the source code, navigate to its directory and proceed to run:
```
pip install cython
pip install -e .
```

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
