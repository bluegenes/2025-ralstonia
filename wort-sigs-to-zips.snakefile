# convert a bunch of .sig files into .sig.zip files and also produce .mf.csv files.
import os

siglist_file="UW551-branchwater-all.wort-siglist.txt"
output_dir="/group/ctbrowngrp5/wort-sra-zips"

rule all:
    input:
       "/group/ctbrowngrp5/wort-sra-zips.mf.csv"
    

def get_accs(wildcards):
    siglist = [x.strip() for x in open(siglist_file)]
    accs = [os.path.basename(x).split('.')[0] for x in siglist] #[:5] # for testing, limit to 5
    return accs

rule make_sig_zip:
    input: "/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{acc}.sig"
    output: "/group/ctbrowngrp5/wort-sra-zips/{acc}.sig.zip" 
    resources:
        slurm_partition="low2",
        mem_mb=lambda wildcards, attempt: attempt * 3000, # 3 GB per attempt
        runtime=60,
    threads: 1
    shell:
        """
        sourmash sig cat {input} -o {output}
        """

rule make_mf_csv:
    input: "/group/ctbrowngrp5/wort-sra-zips/{acc}.sig.zip"
    output: "/group/ctbrowngrp5/wort-sra-zips/{acc}.mf.csv"
    resources:
        slurm_partition="low2",
        mem_mb=lambda wildcards, attempt: attempt * 3000, # 3 GB per attempt
        runtime=240,
    shell:
        """
        sourmash sig collect {input} -o {output} -F csv --abspath
        """

rule aggregate_mf_csv:
    input: expand("/group/ctbrowngrp5/wort-sra-zips/{acc}.mf.csv", acc=get_accs)
    output: "/group/ctbrowngrp5/wort-sra-zips.mf.csv"
    resources:
        slurm_partition="low2",
        mem_mb=lambda wildcards, attempt: attempt * 5000, # 5 GB per attempt
        runtime=240,
    shell:
        """
        sourmash sig collect {input} -o {output} -F csv --abspath
        """