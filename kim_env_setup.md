5. Add ssh key to github repo if required
    see: https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent  
6. Once the ssh key is generated, you can view the hash in a Notebook with:
    ```
    with open("/home/jupyter/.ssh/id_rsa.pub", "r") as f:
        for line in f:
            print(line)
    ``` 
    and paste into github.com credential page
7. git clone the repo
8. cd to the repo and update the base conda environment with
    ```
    conda env update --file environment_tpc.yml --name base
    ```
9. add the repo to the conda path so notebooks will find the package in the python path:  
    `conda develop /home/jupyter/code/bq_snowflake_benchmark`

To understand which program is running in your path:
$ which XXXX
i.e. $ which python
in linux / OSX

$ where XXXX
i.e. >where python
in windows