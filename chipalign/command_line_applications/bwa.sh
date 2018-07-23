#!/usr/bin/env bash
set -e
index=$1
seq=$2
output=$3
nproc=$4
# BWA params from https://github.com/ENCODE-DCC/chip-seq-pipeline2/blob/master/src/encode_bwa.py#L73
bwa aln -q 5 -l 32 -k 2 -t ${nproc} ${index} ${seq} | bwa samse ${index} - ${seq} | samtools view -b - | samtools sort - -o ${output} --threads ${nproc}
