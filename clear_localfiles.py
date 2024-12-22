import subprocess

def git_pull_via_ssh(machine, repo_path):
    try:
        ssh_command = f'ssh ppillai3@{machine} "rm g85/mp4_again_again/localfiles/*; rm g85/mp4_again_again/exec/*"'
        subprocess.run(ssh_command, shell=True, check=True)

    except subprocess.CalledProcessError as e:
        print(f'Failed to pull on {machine}: {e}')


machines = ['fa24-cs425-8501.cs.illinois.edu',
            'fa24-cs425-8502.cs.illinois.edu',
            'fa24-cs425-8503.cs.illinois.edu', 
            'fa24-cs425-8504.cs.illinois.edu',
            'fa24-cs425-8505.cs.illinois.edu',
            'fa24-cs425-8506.cs.illinois.edu', 
            'fa24-cs425-8507.cs.illinois.edu',
            'fa24-cs425-8508.cs.illinois.edu',
            'fa24-cs425-8509.cs.illinois.edu', 
            'fa24-cs425-8510.cs.illinois.edu'
            ]
repo_path = 'g85'


for machine in machines:
    git_pull_via_ssh(machine, repo_path)
    print("Removed files from: " + machine)