"""
Downloading images scrapped from the https://substance3d.adobe.com/assets/allassets
and saved in local SQLite file
"""
import os
import time
import sys

import re
import json

import datetime

import platform

from os import path

import requests  # to get image from the web
import shutil  # to save it locally

from rich import pretty
from rich.console import Console
from rich.traceback import install
from rich.progress import track

from common_database_access import CommonDatabaseAccess

import f_icon

from pathlib import Path

console = Console()
pretty.install()
install()  # this is for tracing project activity
global_data = {"version": "Beta 1 (15.09.2022)\n"}


def clear_console():
    """Clears console view"""
    command = "clear"
    if os.name in ("nt", "dos"):  # If Machine is running on Windows, use cls
        command = "cls"
    os.system(command)


def download_image(url, file_path):
    if not path.exists(file_path):
        r = requests.get(url, stream=True)
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        r.raw.decode_content = True
        # Open a local file with wb ( write binary ) permission.
        with open(file_path, "wb") as f:
            shutil.copyfileobj(r.raw, f)


def append_date(filename):
    """adds date to the end of the filename

    :param str filename: filename
    :return: filename with added current date and time in %Y%m%d-%H%M%S format
    """
    p = Path(filename)
    return "{0}_{2}{1}".format(
        Path.joinpath(p.parent, p.stem), p.suffix, time.strftime("%Y%m%d-%H%M%S")
    )


def check_for_download(url, file_path, need_to_refresh) -> None:
    """
        Checks if destination file exists, and if it does not, or need to refresh, then downloads file from url
    :param str url: url of the file to be downloaded
    :param str file_path: path of the destination file
    :param bool need_to_refresh: force override for download
    """
    if url:
        if os.path.exists(file_path) and need_to_refresh:
            os.rename(file_path, append_date(file_path))
        download_image(url, file_path)


def pluralize(noun) -> str:
    """
    Pluralize noun
    :param noun: noun to be pluralized
    :return: pluralized noun
    """
    if re.search("[sxz]$", noun):
        return re.sub("$", "es", noun)
    elif re.search("[^aeioudgkprt]h$", noun):
        return re.sub("$", "es", noun)
    elif re.search("[aeiou]y$", noun):
        return re.sub("y$", "ies", noun)
    else:
        return noun + "s"


def correct_type_name(type_name) -> str:
    """
    Edit asset type name to folder friendly legacy name
    :param str type_name: type name
    :return: legacy friendly name
    """
    corrected_type = type_name.replace("Substance", "")
    if corrected_type == "IBL":
        corrected_type = "Environment"
    return pluralize(corrected_type)


def check_size(file_path) -> int:
    """
        Checks local file size in bytes
    :param str file_path: path to the file
    :return: file size in bytes
    """
    file_stats = os.stat(file_path)
    return file_stats.st_size


def is_date_early(first_date, second_date) -> bool:
    """
        Compares 2 strings as dates
    :param str first_date: first date
    :param str second_date: second date
    :return: return true if first date is earlier than second date
    """
    first_converted_date = datetime.datetime.strptime(
        first_date, "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    second_converted_date = datetime.datetime.strptime(
        second_date, "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    return first_converted_date < second_converted_date


def move_folders_to_new_category(database) -> None:
    """
    Checks if asset folder do not exist at category location, then looks in every category
    for the asset to relocate to the proper location
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Generating report ...")
    asset_types = database.get_all_types()
    all_categories = database.get_all_categories()
    placement_log = []
    for a in asset_types:  # track(asset_types, description="Types."):
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            expected_path = (
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
                + os.sep
                + asset["name"]
            )
            if not os.path.exists(expected_path):
                # we did not find our asset in the right place, so we check everywhere
                found = False
                for a1 in asset_types:
                    for c1 in all_categories:
                        checked_path = (
                            global_data["local_path"]
                            + os.sep
                            + correct_type_name(a1["name"])
                            + os.sep
                            + c1["name"]
                            + os.sep
                            + asset["name"]
                        )
                        if checked_path != expected_path and os.path.exists(
                            checked_path
                        ):
                            placement_log.append(checked_path + " >> " + expected_path)
                            if not os.path.exists(
                                global_data["local_path"]
                                + os.sep
                                + correct_type_name(a["name"])
                                + os.sep
                                + c["name"]
                            ):
                                os.makedirs(
                                    global_data["local_path"]
                                    + os.sep
                                    + correct_type_name(a["name"])
                                    + os.sep
                                    + c["name"]
                                )

                            os.rename(checked_path, expected_path)
                            found = True
                            break
                    if found:
                        break
    console.print("Moved Assets - " + str(len(placement_log)))
    console.print()
    console.print("All Done !!!")
    if len(placement_log) > 0:
        file = open(
            append_date(
                global_data["local_path"] + os.sep + "AssetCategoryChangeLog.txt"
            ),
            "w",
            encoding="utf-8",
        )
        for f in placement_log:
            file.write(f + "\n")
        file.close()
    input("Press any enter to close...")


def fancy_list_generation(database) -> None:
    """
        Generates txt file with asset name, types and url based on Requests.txt for easy findings
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Generating request list ...")
    fancy_requests = []
    if os.path.exists(global_data["local_path"] + os.sep + "Requests.txt"):
        with open(global_data["local_path"] + os.sep + "Requests.txt") as f:
            base_requests = f.read().splitlines()
        for base_r in track(
            base_requests, description="Requests.", total=len(base_requests)
        ):
            asset = database.get_asset_revision_by_name(base_r)
            base_asset = database.get_asset_by_asset_id(asset[0]["asset_id"])
            asset_downloads = database.get_asset_download_by_asset_id(
                asset[0]["asset_id"]
            )
            asset_format = ""
            download_tags = []
            for ad in asset_downloads:
                tags = database.get_download_download_tag_by_download_id(
                    ad["download_id"]
                )
                for t in tags:
                    tag = database.get_download_tag_by_download_tag_id(
                        t["download_tag_id"]
                    )
                    if (
                        tag[0]["name"] != "default"
                        and tag[0]["name"] != "download"
                        and tag[0]["name"] not in download_tags
                    ):
                        download_tags.append(tag[0]["name"])
            for dt in download_tags:
                asset_format += dt + " "
            fancy_requests.append(
                asset[0]["name"]
                + " - "
                + asset_format.strip()
                + " - "
                + r"https://substance3d.adobe.com/assets/allassets/"
                + base_asset[0]["original_id"]
            )
    if len(fancy_requests) > 0:
        file = open(
            append_date(global_data["local_path"] + os.sep + "Result.txt"),
            "w",
            encoding="utf-8",
        )
        for f in fancy_requests:
            file.write(f + "\n")
        file.close()
    input("Press any enter to close...")


def generate_detail_report(database) -> None:
    """
        Generates report txt with existing, missing and revision assets files
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Generating detail report ...")
    asset_types = database.get_all_types()
    placement_log = {"have": [], "missing": [], "revision": []}
    for a in asset_types:  # track(asset_types, description="Types."):
        if not os.path.exists(
            global_data["local_path"] + os.sep + correct_type_name(a["name"])
        ):
            continue
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        data = {}
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            if os.path.exists(
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
                + os.sep
                + asset["name"]
            ):
                asset_downloads = database.get_asset_download_by_asset_id(
                    asset["asset_id"]
                )
                for ad in asset_downloads:
                    revisions = database.get_revision_by_download_id(ad["download_id"])
                    max_revision = 0
                    max_date = ""
                    found_file_revision = -1
                    found_date = ""
                    for r in revisions:
                        max_revision = max(max_revision, r["revision"])
                        if r["revision"] == max_revision:
                            max_date = r["created_at"]
                        if r["have_file"]:
                            found_file_revision = max(
                                found_file_revision, r["revision"]
                            )
                            found_date = r["created_at"]
                    if found_file_revision > -1:
                        # we found file
                        if found_file_revision == max_revision:
                            if max_date == found_date:
                                placement_log["have"].append(
                                    f"{correct_type_name(a['name'])} > {c['name']} > {asset['name']} > {revisions[0]['filename']}"
                                )
                            elif is_date_early(found_date, max_date):
                                placement_log["revision"].append(
                                    f"{correct_type_name(a['name'])} > {c['name']} > {asset['name']} > {revisions[0]['filename']} -- Have Revision {found_file_revision} with date {found_date}, max revision {max_revision} with date {max_date}"
                                )
                            else:
                                # this not suppose to happen
                                placement_log["have"].append(
                                    f"{correct_type_name(a['name'])} > {c['name']} > {asset['name']} > {revisions[0]['filename']}"
                                )
                        else:
                            placement_log["revision"].append(
                                f"{correct_type_name(a['name'])} > {c['name']} > {asset['name']} > {revisions[0]['filename']} -- Have Revision {found_file_revision}, max revision {max_revision}"
                            )
                    else:
                        placement_log["missing"].append(
                            f"{correct_type_name(a['name'])} > {c['name']} > {asset['name']} > {revisions[0]['filename']}"
                        )
    file = open(
        append_date(global_data["local_path"] + os.sep + "AssetDetailsCountReport.txt"),
        "w",
        encoding="utf-8",
    )
    console.print("Have assets - " + str(len(placement_log["have"])))
    console.print("Missing assets - " + str(len(placement_log["missing"])))
    console.print("Revision assets - " + str(len(placement_log["revision"])))
    if len(placement_log["have"]) > 0:
        file.write(f'Have assets({len(placement_log["have"])}): \n')
        file.write("\n")
        for f in placement_log["have"]:
            file.write(f + "\n")
        file.write("\n")
    if len(placement_log["missing"]) > 0:
        file.write(f'Missing assets({len(placement_log["missing"])}): \n')
        file.write("\n")
        for f in placement_log["missing"]:
            file.write(f + "\n")
        file.write("\n")
    if len(placement_log["revision"]) > 0:
        file.write(f'Revision assets({len(placement_log["revision"])}): \n')
        file.write("\n")
        for f in placement_log["revision"]:
            file.write(f + "\n")
        file.write("\n")
    file.close()
    input("Press any enter to close...")


def generate_folder_report(database) -> None:
    """
        generates report on existing asset folders
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Generating folder report ...")
    asset_types = database.get_all_types()
    placement_log = []
    for a in asset_types:  # track(asset_types, description="Types."):
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        data = {}
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            local_path = (
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
                + os.sep
                + asset["name"]
            )
            if c["name"] not in data:
                data[c["name"]] = {"have": 0, "missing": 0}
            if os.path.exists(local_path):
                data[c["name"]]["have"] = data[c["name"]]["have"] + 1
            else:
                data[c["name"]]["missing"] = data[c["name"]]["missing"] + 1
        for d in data:
            placement_log.append(
                f"{correct_type_name(a['name'])} - {d} (Have {data[d]['have']}; Missing {data[d]['missing']})"
            )
    file = open(
        append_date(global_data["local_path"] + os.sep + "AssetFolderCountReport.txt"),
        "w",
        encoding="utf-8",
    )
    for f in placement_log:
        file.write(f + "\n")
    file.close()
    input("Press any enter to close...")


def mark_database_with_my_files(database) -> None:
    """
        Marks database with locally existing asset files
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Checking local files for the database ...")
    placement_log = {"new": []}
    asset_types = database.get_all_types()
    for a in asset_types:  # track(asset_types, description="Types."):
        if not os.path.exists(
            global_data["local_path"] + os.sep + correct_type_name(a["name"])
        ):
            continue
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            if os.path.exists(
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
            ):
                local_path = (
                    global_data["local_path"]
                    + os.sep
                    + correct_type_name(a["name"])
                    + os.sep
                    + c["name"]
                    + os.sep
                    + asset["name"]
                    + os.sep
                )
                asset_downloads = database.get_asset_download_by_asset_id(
                    asset["asset_id"]
                )
                for ad in asset_downloads:
                    revisions = database.get_revision_by_download_id(ad["download_id"])
                    # revisions can have different file names, so need to check everything
                    for r in revisions:
                        check_file = local_path + r["filename"]
                        if os.path.exists(check_file):
                            file_size = check_size(check_file)
                            if r["size"] == file_size and not r["have_file"]:
                                r["have_file"] = True
                                database.update_revision(r)
                                placement_log["new"].append(f"{check_file}")
                                # break
    console.print("New files - " + str(len(placement_log["new"])))
    console.print()
    input("Press any enter to close...")


def transfer_all_local_files(database) -> None:
    """
        Moves all local asset files in _source folder to appropriate asset folder
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Placing files in corresponding folders ...")
    files = os.listdir(global_data["local_path"] + os.sep + global_data["source_path"])
    placement_log = {"moved": [], "double": [], "missing": [], "revision": []}
    for f in files:
        file_revision = database.get_revision_by_filename(f)
        if len(file_revision) > 0:
            download = database.get_download_by_download_id(
                file_revision[0]["download_id"]
            )
            asset_download = database.get_asset_download_by_download_id(
                download[0]["download_id"]
            )
            asset_original_id = database.get_asset_by_asset_id(
                asset_download[0]["asset_id"]
            )
            asset_revision = database.get_latest_asset_revision_by_original_id(
                asset_original_id[0]["original_id"]
            )
            asset_type = database.get_types_by_type_id(asset_revision[0]["type_id"])
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset_revision[0]["asset_id"])[
                    0
                ]["category_id"]
            )[0]
            source_path = (
                global_data["local_path"]
                + os.sep
                + global_data["source_path"]
                + os.sep
                + f
            )
            local_path = (
                global_data["local_path"]
                + os.sep
                + correct_type_name(asset_type[0]["name"])
                + os.sep
                + c["name"]
                + os.sep
                + asset_revision[0]["name"]
            )
            destination_path = local_path + os.sep + f
            if os.path.exists(local_path):
                if os.path.exists(destination_path):
                    # destination file exists
                    new_size = check_size(source_path)
                    old_size = check_size(destination_path)
                    new_rev = 0
                    old_rev = 0
                    if new_size != old_size:
                        # different sizes
                        for r in file_revision:
                            if r["size"] == new_size:
                                new_rev = r["revision"]
                            if r["size"] == old_size:
                                old_rev = r["revision"]
                        if new_rev > old_rev:
                            # source is new file
                            for_rename = os.path.splitext(destination_path)
                            os.rename(
                                destination_path,
                                f"{for_rename[0]}_rev{old_rev}{for_rename[1]}",
                            )
                            os.rename(source_path, destination_path)
                            placement_log["revision"].append(
                                f"{f} -> {correct_type_name(asset_type[0]['name'])} -- {c['name']} -- {asset_revision[0]['name']}. Replacing {for_rename[0]}_rev{old_rev}{for_rename[1]}"
                            )
                        else:
                            # source is old file
                            for_rename = os.path.splitext(destination_path)
                            os.rename(
                                source_path,
                                f"{for_rename[0]}_rev{new_rev}{for_rename[1]}",
                            )
                            placement_log["revision"].append(
                                f"{for_rename[0]}_rev{old_rev}{for_rename[1]} -> {correct_type_name(asset_type[0]['name'])} -- {c['name']} -- {asset_revision[0]['name']}"
                            )
                    else:
                        # same sizes
                        placement_log["double"].append(
                            f"{f} -> {correct_type_name(asset_type[0]['name'])} -- {c['name']} -- {asset_revision[0]['name']}"
                        )
                else:
                    # destination file do not exist
                    os.rename(source_path, destination_path)
                    placement_log["moved"].append(
                        f"{f} -> {correct_type_name(asset_type[0]['name'])} -- {c['name']} -- {asset_revision[0]['name']}"
                    )
            else:
                # destination folder do not exist
                placement_log["missing"].append(
                    f"Missing folder for -> {correct_type_name(asset_type[0]['name'])} -- {c['name']} -- {asset_revision[0]['name']}. For File -> {f}"
                )

    # generating report
    if (
        len(placement_log["moved"]) > 0
        or len(placement_log["revision"]) > 0
        or len(placement_log["double"]) > 0
        or len(placement_log["missing"]) > 0
    ):
        file = open(
            append_date(global_data["local_path"] + os.sep + "FileTransferReport.txt"),
            "w",
            encoding="utf-8",
        )
        if len(placement_log["moved"]) > 0:
            file.write(f'Moved files({len(placement_log["moved"])}): \n')
            file.write("\n")
            for f in placement_log["moved"]:
                file.write(f + "\n")
            file.write("\n")
        if len(placement_log["revision"]) > 0:
            file.write(f'Added Revision({len(placement_log["revision"])}): \n')
            file.write("\n")
            for f in placement_log["revision"]:
                file.write(f + "\n")
            file.write("\n")
        if len(placement_log["double"]) > 0:
            file.write(f'Doubles, not moved({len(placement_log["double"])}): \n')
            file.write("\n")
            for f in placement_log["double"]:
                file.write(f + "\n")
            file.write("\n")
        if len(placement_log["missing"]) > 0:
            file.write(
                f'Missing locations for files({len(placement_log["missing"])}): \n'
            )
            file.write("\n")
            for f in placement_log["missing"]:
                file.write(f + "\n")
        file.close()

    console.print("Moved files - " + str(len(placement_log["moved"])))
    console.print("Revision files - " + str(len(placement_log["revision"])))
    console.print("Double files (not moved) - " + str(len(placement_log["double"])))
    console.print("Missing Destinations - " + str(len(placement_log["missing"])))
    console.print()
    console.print("All Done !!!")
    input("Press any enter to close...")


def make_all_icons(database, ignore_created=True) -> None:
    """
        Generate icons for all asset folders
    :param CommonDatabaseAccess database: reference to the database
    :param bool ignore_created: ignores already existing icons if True (default)
    """
    console.print("Creating folder icons ...")
    asset_types = database.get_all_types()
    for a in asset_types:  # track(asset_types, description="Types."):
        if not os.path.exists(
            global_data["local_path"] + os.sep + correct_type_name(a["name"])
        ):
            continue
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            if os.path.exists(
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
            ):
                local_path = (
                    global_data["local_path"]
                    + os.sep
                    + correct_type_name(a["name"])
                    + os.sep
                    + c["name"]
                    + os.sep
                    + asset["name"]
                )
                if os.path.exists(local_path):
                    # console.print(asset)
                    if platform.system() == "Windows":
                        if os.path.exists(local_path + os.sep + "Preview.png") and (
                            not os.path.exists(local_path + os.sep + "Preview.ico")
                            or ignore_created
                        ):
                            f_icon.create_icon(local_path + os.sep + "Preview.png")
                    else:
                        if os.path.exists(local_path + os.sep + "Preview.png"):
                            f_icon.create_icon(local_path + os.sep + "Preview.png")

    input("Press any enter to close...")


def download_all_images(database) -> None:
    """
        Downloads all images for each asset folder and generates extra_data.txt file
    :param CommonDatabaseAccess database: reference to the database
    """
    console.print("Downloading images ...")
    asset_types = database.get_all_types()
    far_tag_id = database.get_all_preview_tag_by_name("far")[0]["preview_tag_id"]
    for a in asset_types:  # track(asset_types, description="Types."):
        if not os.path.exists(
            global_data["local_path"] + os.sep + correct_type_name(a["name"])
        ):
            continue
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            if os.path.exists(
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
            ):
                local_path = (
                    global_data["local_path"]
                    + os.sep
                    + correct_type_name(a["name"])
                    + os.sep
                    + c["name"]
                    + os.sep
                    + asset["name"]
                )
                if os.path.exists(local_path):
                    extra_data_path = local_path + os.sep + "extra-data.txt"
                    extra_data = {}
                    if os.path.exists(extra_data_path):
                        with open(extra_data_path) as json_file:
                            extra_data = json.load(json_file)
                    if "preview_details" not in extra_data:
                        extra_data["preview_details"] = []
                    if "preview_variant" not in extra_data:
                        extra_data["preview_variant"] = []
                    if "extra_data" not in extra_data:
                        extra_data["extra_data"] = {}
                    asset_previews = database.get_asset_preview_by_asset_id(
                        asset["asset_id"]
                    )
                    for ap in asset_previews:
                        preview = database.get_preview_by_preview_id(ap["preview_id"])[
                            0
                        ]
                        if preview["preview_id"] == asset["thumbnail_id"]:
                            if (
                                "preview_original_id" not in extra_data
                                or extra_data["preview_original_id"]
                                != preview["original_id"]
                            ):
                                check_for_download(
                                    preview["url"],
                                    local_path + os.sep + "Preview.png",
                                    True,
                                )
                                extra_data["preview_original_id"] = preview[
                                    "original_id"
                                ]
                                f_icon.create_icon(local_path + os.sep + "Preview.png")
                        else:
                            if (
                                preview["preview_id"]
                                not in extra_data["preview_details"]
                                and preview["preview_id"]
                                not in extra_data["preview_variant"]
                            ):
                                is_far = (
                                    len(
                                        database.get_preview_preview_tag_by_preview_id_and_preview_tag_id(
                                            preview["preview_id"], far_tag_id
                                        )
                                    )
                                    > 0
                                )
                                if is_far:
                                    details_count = len(extra_data["preview_details"])
                                    check_for_download(
                                        preview["url"],
                                        local_path
                                        + os.sep
                                        + f"Details{'' if details_count == 0 else str(details_count)}.png",
                                        True,
                                    )
                                    extra_data["preview_details"].append(
                                        preview["original_id"]
                                    )
                                else:
                                    variant_count = len(extra_data["preview_variant"])
                                    check_for_download(
                                        preview["url"],
                                        local_path
                                        + os.sep
                                        + f"Variant{str(variant_count + 1)}.png",
                                        True,
                                    )
                                    extra_data["preview_variant"].append(
                                        preview["original_id"]
                                    )
                    if asset["extra_data_author"]:
                        extra_data["extra_data"]["author"] = asset["extra_data_author"]
                    if asset["extra_data_physical_size"]:
                        extra_data["extra_data"]["physical_size"] = asset[
                            "extra_data_physical_size"
                        ]
                    if asset["extra_data_type"]:
                        extra_data["extra_data"]["type"] = asset["extra_data_type"]
                    if asset["extra_data_style"]:
                        extra_data["extra_data"]["style"] = asset["extra_data_style"]
                    if asset["extra_data_quality"]:
                        extra_data["extra_data"]["quality"] = asset[
                            "extra_data_quality"
                        ]
                    if asset["extra_data_meshes"]:
                        extra_data["extra_data"]["meshes"] = asset["extra_data_meshes"]
                    if asset["extra_data_counters_quads"]:
                        extra_data["extra_data"]["quads"] = asset[
                            "extra_data_counters_quads"
                        ]
                    if asset["extra_data_substance_resolution"]:
                        extra_data["extra_data"]["substance_resolution"] = asset[
                            "extra_data_substance_resolution"
                        ]
                    if asset["extra_data_preview_disp"]:
                        extra_data["extra_data"]["preview_displacement"] = asset[
                            "extra_data_preview_disp"
                        ]
                    with open(extra_data_path, "w") as outfile:
                        json.dump(extra_data, outfile, indent=4, sort_keys=True)

    input("Press any enter to close...")


def create_folder_for_type(database, asset_types) -> None:
    """
        Generates folders for each asset in given asset type
    :param CommonDatabaseAccess database: reference to the database
    :param [] asset_types: given asset types
    """
    # 1. create _source folder for files to move to their location
    if not os.path.exists(
        global_data["local_path"] + os.sep + global_data["source_path"]
    ):
        os.makedirs(global_data["local_path"] + os.sep + global_data["source_path"])
    # 2. Now creating rest of the folders
    console.print("Creating folders ...")
    for a in asset_types:  # track(asset_types, description="Types."):
        all_type_assets = database.get_all_assets_revisions_by_type_id(a["type_id"])
        if not os.path.exists(
            global_data["local_path"] + os.sep + correct_type_name(a["name"])
        ):
            os.makedirs(
                global_data["local_path"] + os.sep + correct_type_name(a["name"])
            )
        console.print()
        for asset in track(
            all_type_assets,
            description=f"Assets for type {correct_type_name(a['name'])}",
            total=len(all_type_assets),
        ):
            c = database.get_category_by_id(
                database.get_asset_category_by_asset_id(asset["asset_id"])[0][
                    "category_id"
                ]
            )[0]
            if not os.path.exists(
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
            ):
                os.makedirs(
                    global_data["local_path"]
                    + os.sep
                    + correct_type_name(a["name"])
                    + os.sep
                    + c["name"]
                )
            if not os.path.exists(
                global_data["local_path"]
                + os.sep
                + correct_type_name(a["name"])
                + os.sep
                + c["name"]
                + os.sep
                + asset["name"]
            ):
                os.makedirs(
                    global_data["local_path"]
                    + os.sep
                    + correct_type_name(a["name"])
                    + os.sep
                    + c["name"]
                    + os.sep
                    + asset["name"]
                )
    console.print()
    input("Press any enter to close...")


def create_folders(database) -> None:
    """
        Draw meny for asset type selection for folder creation
    :param CommonDatabaseAccess database: reference to the database
    """
    menu_title = " Select asset type to create folder"
    count = 1
    menu_items = []
    all_asset_types = database.get_all_types()
    for asset_type in all_asset_types:
        menu_items.append(f"[{count}] {correct_type_name(asset_type['name'])}")
        count = count + 1
    menu_items.append(f"[{count}] All")
    count = count + 1
    menu_items.append(f"[{count}] Return")

    menu_exit = False
    while not menu_exit:
        # cls()
        clear_console()
        console.print("version " + global_data["version"])
        console.print(menu_title + "")
        for m_i in menu_items:
            console.print(m_i + "")
        console.print("")
        user_input = input("Enter a number: ")
        if user_input.isnumeric():
            menu_sel = int(user_input)
            if 1 <= menu_sel < count - 1:  # Specific asset type
                create_folder_for_type(database, [all_asset_types[menu_sel - 1]])
            elif menu_sel == count - 1:  # all asset types
                create_folder_for_type(database, all_asset_types)
            elif menu_sel == count:  # Quit
                menu_exit = True


def main_menu(database) -> None:
    """
    Draw main menu
    :param CommonDatabaseAccess database: reference to the database
    """
    menu_title = " Select action"
    menu_items = [
        "[1] Create folders.",
        "[2] Download all images.",
        "[3] Make all icons. Where Preview.ico do not exist.",
        "[4] Make all icons, but ignore where Preview.ico exists.",
        "[5] Transfer all local files from _source folder to appropriate folders.",
        "[6] Mark database with my files. (Do this before Generating report).",
        "[7] Generate all folder report. (Do this after Marking database with my files).",
        "[8] Generate existing folder report. (Do this after Marking database with my files).",
        "[9] Fancy list generation. (Convert simple material list to list with format and links, looks for Requests.txt).",
        "[10] Move folders if Category changed.",
        "[11] Quit.",
    ]
    menu_exit = False
    while not menu_exit:
        clear_console()
        console.print("version " + global_data["version"])
        console.print(menu_title + "")
        for m_i in menu_items:
            console.print(m_i + "")
        console.print("")
        user_input = input("Enter a number: ")
        if user_input.isnumeric():
            menu_sel = int(user_input)
            if menu_sel == 1:  # Create folders
                create_folders(database)
            if menu_sel == 2:  # Download all images
                download_all_images(database)
            if menu_sel == 3:  # Make all icons
                make_all_icons(database, False)
            if menu_sel == 4:  # Make all icons
                make_all_icons(database)
            if menu_sel == 5:  # Transfer all local files
                transfer_all_local_files(database)
            if menu_sel == 6:  # Mark database with my files
                mark_database_with_my_files(database)
            if menu_sel == 7:  # Generate folder report
                generate_folder_report(database)
            if menu_sel == 8:  # Generate detail report
                generate_detail_report(database)
            if menu_sel == 9:  # Fancy list generation
                fancy_list_generation(database)
            if menu_sel == 10:  # Move folders to new category
                move_folders_to_new_category(database)
            if menu_sel == 11:  # Quit
                menu_exit = True


def main() -> None:
    """
    Check location of the database and then going to main menu
    """
    menu_title = " Select database file"
    menu_items = []
    menu_items_count = 0
    menu_items_references = []
    local_path = os.path.dirname(sys.argv[0])
    global_data["local_path"] = local_path
    global_data["source_path"] = "_source"
    files = os.listdir(local_path)

    # generate fake file in given size for testing
    # f = open(f"{global_data['local_path']+os.sep}suede_zigzag_quilt.sbsar", "wb")
    # f.seek(24042746 - 1)
    # f.write(b"\0")
    # f.close()

    for f in files:
        file_details = os.path.splitext(f)
        if os.path.isfile(local_path + os.sep + f) and file_details[1] == ".db":
            menu_items.append(f"[{menu_items_count + 1}] {f}")
            menu_items_count = menu_items_count + 1
            menu_items_references.append(f)
    if menu_items_count == 0:
        clear_console()
        console.print("Database files not found next to the application files.")
        input("Press any enter to close...")
    elif menu_items_count == 1:
        database = CommonDatabaseAccess(
            db_path=local_path + os.sep + menu_items_references[0], force=False
        )
        main_menu(database)
    else:
        menu_exit = False
        while not menu_exit:
            clear_console()
            console.print("version " + global_data["version"])
            console.print(menu_title + "")
            for m_i in menu_items:
                console.print(m_i + "")
            console.print("")
            user_input = input("Enter a number: ")
            if user_input.isnumeric():
                menu_sel = int(user_input)
                if 0 < menu_sel <= len(menu_items_references):  # Initial scan
                    database = CommonDatabaseAccess(
                        db_path=local_path
                        + os.sep
                        + menu_items_references[menu_sel - 1],
                        force=False,
                    )
                    main_menu(database)
                    menu_exit = True


if __name__ == "__main__":
    main()
