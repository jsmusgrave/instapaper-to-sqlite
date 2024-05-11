import click
import pathlib
import json
import sqlite_utils
from datetime import datetime
from instapaper_to_sqlite import utils
from pyinstapaper.instapaper import Instapaper, Bookmark


@click.group()
@click.version_option()
def cli():
    "Save data from Instapaper to a SQLite database"


@cli.command()
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    default="auth.json",
    help="Path to save tokens to, defaults to ./auth.json.",
)
def auth(auth):
    "Save authentication credentials to a JSON file"
    auth_data = {}
    if pathlib.Path(auth).exists():
        auth_data = json.load(open(auth))
    click.echo(
        "In Instapaper, get a Full API key following the process at https://www.instapaper.com/api."
    )
    consumer_id = click.prompt("OAuth Consumer ID")
    consumer_secret = click.prompt("OAuth Consumer Secret")
    login = click.prompt("Instapaper login (email)")
    password = click.prompt("Instapaper password", hide_input=True)
    auth_data.update(
        {
            "instapaper_consumer_id": consumer_id,
            "instapaper_consumer_secret": consumer_secret,
            "instapaper_email": login,
            "instapaper_password": password,
        }
    )

    open(auth, "w").write(json.dumps(auth_data, indent=4) + "\n")
    click.echo()
    click.echo(
        "Your authentication credentials have been saved to {}. You can now import articles by running:".format(
            auth
        )
    )
    click.echo()
    click.echo("    $ instapaper-to-sqlite bookmarks instapaper.db")


BOOKMARK_KEYS = [
    "bookmark_id",
    "title",
    "description",
    "hash",
    "url",
    "progress_timestamp",
    "time",
    "progress",
    "starred",
    "type",
    "private_source",
    # "folder"
]

BOOKMARK_TEXT_KEYS = [
    "bookmark_id",
    "text",
]

@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    default="auth.json",
    help="Path to save tokens to, defaults to auth.json",
)
@click.option(
    "-f",
    "--folder",
    default="archive",
    help="The folder of bookmarks to save",
)
def bookmarks(db_path, auth, folder):
    """Save a folder of bookmarks"""
    db = sqlite_utils.Database(db_path)
    try:
        data = json.load(open(auth))
        consumer_id = data["instapaper_consumer_id"]
        consumer_secret = data["instapaper_consumer_secret"]
        login = data["instapaper_email"]
        password = data["instapaper_password"]
    except (KeyError, FileNotFoundError):
        utils.error(
            "Cannot find authentication data, please run `instapaper-to-sqlite auth`!"
        )
    print("Fetching bookmarks...")
    instapaper = Instapaper(consumer_id, consumer_secret)
    instapaper.login(login, password)

    bookmarks = [
        {key: getattr(entry, key) for key in BOOKMARK_KEYS}
        for entry in instapaper.get_bookmarks(folder, limit=500)
    ]
    print("Downloaded {} bookmarks from folder '{}'.".format(len(bookmarks), folder))
    for b in bookmarks:
        b.update({"folder": folder})
    db["bookmarks"].upsert_all(bookmarks, pk="bookmark_id", alter=True)

@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    default="auth.json",
    help="Path to save tokens to, defaults to auth.json",
)
@click.option(
    "-t",
    "--trace",
    is_flag=True,
    help="Option to print tracing",
)
def get_text(db_path, auth, trace):
    """Download text for individual bookmarks"""
    db = sqlite_utils.Database(db_path)
    try:
        data = json.load(open(auth))
        consumer_id = data["instapaper_consumer_id"]
        consumer_secret = data["instapaper_consumer_secret"]
        login = data["instapaper_email"]
        password = data["instapaper_password"]
    except (KeyError, FileNotFoundError):
        utils.error(
            "Cannot find authentication data, please run `instapaper-to-sqlite auth`!"
        )

    # Create bookmark_text table if not exist table
    db["bookmark_text"].create({
    "bookmark_id": int,
    "text": str,
    "error": bool,
        }, pk="bookmark_id", if_not_exists=True, foreign_keys=[("bookmark_id", "bookmarks", "bookmark_id")])
    
    the_unpopulated_bookmarks_query = "select b.* from bookmarks b left join bookmark_text bt on b.bookmark_id = bt.bookmark_id where bt.text is null;"
    unpopulated_bookmarks = list(db.query(the_unpopulated_bookmarks_query))
    instapaper = Instapaper(consumer_id, consumer_secret)
    instapaper.login(login, password)

    if trace: print(f"Iterating through {len(unpopulated_bookmarks)} bookmarks without text or errors.")
    for num, row in enumerate(unpopulated_bookmarks):
        isoDateToTimestap = lambda x : datetime.strptime(x, "%Y-%m-%dT%H:%M:%S").timestamp()
        str_dict = {key: str(value) for key, value in row.items()}
        str_dict["progress_timestamp"] = isoDateToTimestap(str_dict["progress_timestamp"])
        str_dict["time"] =  isoDateToTimestap(str_dict["time"])

        try :
            bookmark_count_text = f"{num} of {len(unpopulated_bookmarks)}"
            print(f"Pulling Text for Bookmark {bookmark_count_text}")
            if trace: print(f"Querying for bookmark_id: {str_dict['bookmark_id']} title: {str_dict['title']}")
            bookmark = Bookmark(instapaper, **str_dict)
            txt = bookmark.get_text()['data'].decode('utf-8')
            db["bookmark_text"].insert({"bookmark_id":str_dict["bookmark_id"], "text":txt, "error":False})
            if trace: print(f"For {bookmark_count_text} recieved text of {len(txt)} bytes")
        except Exception as e:
            print(f"Caught Exception querying bookmark {bookmark_count_text}.  Exception: {e}")
            db["bookmark_text"].insert({"bookmark_id":str_dict["bookmark_id"], "text":"", "error":True})
    if trace: print(f"Finished downloading text")



if __name__ == "__main__":
    cli()
