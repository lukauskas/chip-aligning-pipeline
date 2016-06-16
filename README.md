# chip-aligning-pipeline
Pipeline for aligning ChIP seq reads from SRA files to reference genome

# Installation

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
