from prefect import flow, task
import subprocess

@task
def run_bash_command():
    result = subprocess.run(["ls", "-lah"], capture_output=True, text=True)
    print(result.stdout)

@task
def run_python_task():
    print("Running Python Task!")

@flow
def my_workflow():
    run_bash_command()
    run_python_task()

if __name__ == "__main__":
    my_workflow.serve(name="my-first-deployment", cron="0 * * * *")  # Runs every hour
