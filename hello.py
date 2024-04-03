from prefect import flow, task
from prefect.blocks.system import String

string_block = String.load("future-nba-champs")

@task
def create_message():
    msg = string_block.value
    return msg


@flow
def something_else():
    return 10


@flow
def hello_world():
    print(str(something_else()) + create_message())


if __name__ == "__main__":
    hello_world()