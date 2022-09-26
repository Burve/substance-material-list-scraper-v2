"""Scraping https://substance3d.adobe.com/assets/allassets for all offered asset information"""
import os
import sys
import argparse

import json
import requests
import time
from datetime import datetime

from os import path
from pathlib import Path

from rich import pretty
from rich.console import Console
from rich.traceback import install
from rich.progress import track

from common_database_access import CommonDatabaseAccess


console = Console()
pretty.install()
install()  # this is for tracing project activity
global_data = {"version": "Beta 1 (08.09.2022)\n"}


def get_duration(then, now=datetime.now(), interval="default"):
    # Returns a duration as specified by variable interval
    # Functions, except totalDuration, returns [quotient, remainder]

    duration = now - then  # For build-in functions
    duration_in_s = duration.total_seconds()
    if duration_in_s < 0:
        duration_in_s = 0

    def years():
        return divmod(duration_in_s, 31536000)  # Seconds in a year=31536000.

    def days(seconds=None):
        return divmod(
            seconds if seconds != None else duration_in_s, 86400
        )  # Seconds in a day = 86400

    def hours(seconds=None):
        return divmod(
            seconds if seconds != None else duration_in_s, 3600
        )  # Seconds in an hour = 3600

    def minutes(seconds=None):
        return divmod(
            seconds if seconds != None else duration_in_s, 60
        )  # Seconds in a minute = 60

    def seconds(seconds=None):
        if seconds != None:
            return divmod(seconds, 1)
        return duration_in_s

    def total_duration():
        y = years()
        d = days(y[1])  # Use remainder to calculate next variable
        h = hours(d[1])
        m = minutes(h[1])
        s = seconds(m[1])

        # return "Time between dates: {} years, {} days, {} hours, {} minutes and {} seconds".format(int(y[0]), int(d[0]),
        return " {} years, {} days, {} hours, {} minutes and {} seconds".format(
            int(y[0]), int(d[0]), int(h[0]), int(m[0]), int(s[0])
        )

    return {
        "years": int(years()[0]),
        "days": int(days()[0]),
        "hours": int(hours()[0]),
        "minutes": int(minutes()[0]),
        "seconds": int(seconds()),
        "default": total_duration(),
    }[interval]


def append_date(filename):
    """adds date to the end of the filename

    :param str filename: filename
    :return: filename with added current date and time in %Y%m%d-%H%M%S format
    """
    p = Path(filename)
    return "{0}_{2}{1}".format(
        Path.joinpath(p.parent, p.stem), p.suffix, time.strftime("%Y%m%d-%H%M%S")
    )


def clear_console():
    """Clears console view"""
    command = "clear"
    if os.name in ("nt", "dos"):  # If Machine is running on Windows, use cls
        command = "cls"
    os.system(command)


def scrap_online_data():
    """
    Access Substance material list webpage APK for all asset details.
    """
    items = []
    page = 0
    url = "https://source-api.substance3d.com/beta/graphql"
    # 1. Downloading all elements from the API. 100 elements at a time.
    while True:
        payload = {
            "operationName": "Assets",
            "query": "query Assets($page: Int = 0, $limit: Int = 100, $search: String, $filters: AssetFilters, $sortDir: SortDir = desc, $sort: AssetSort = byPublicationDate) {\n  assets(search: $search, filters: $filters, sort: $sort, sortDir: $sortDir, page: $page, limit: $limit) {\n    total\n    hasMore\n    items {\n      ...AssetAttachmentsFragment\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment AssetAttachmentsFragment on Asset {\n  ...AssetFragment\n  attachments {\n    id\n    tags\n    label\n    ... on PreviewAttachment {\n      kind\n      url\n      __typename\n    }\n    ... on DownloadAttachment {\n      url\n      revisions {\n        filename\n        size\n        revision\n        createdAt\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment AssetFragment on Asset {\n  id\n  title\n  tags\n  type\n  status\n  categories\n  cost\n  new\n  free\n  licenses\n  downloadsRecentlyUpdated\n  extraData {\n    key\n    value\n    __typename\n  }\n  thumbnail {\n    id\n    url\n    tags\n    __typename\n  }\n  createdAt\n  __typename\n}\n",
        }
        q = payload["query"]
        payload["query"] = q.replace("$page: Int = 0,", f"$page: Int = {page},")
        r = requests.post(
            url, headers={"Origin": "https://substance3d.adobe.com"}, data=payload
        )
        data = json.loads(r.text)
        items.extend(data["data"]["assets"]["items"])
        print(
            f"Downloaded assets data - {len(items)} / {data['data']['assets']['total']}"
        )
        if data["data"]["assets"]["hasMore"]:
            page = page + 1
            time.sleep(0.1)
        else:
            break
    with open(global_data["data_path"], "w+") as convert_file:
        convert_file.write(json.dumps(items))
    console.print()
    console.print("All Done !!!")
    input("Press Enter to continue...")


def process_online_data(database):
    """
    Processes saved online data
    :param CommonDatabaseAccess database: reference to teh database
    """
    if not os.path.exists(global_data["data_path"]):
        console.print("Missing data file, download it first !!!\n")
        return
    count = 0
    all_tags = database.get_all_tags()
    all_types = database.get_all_types()
    all_categories = database.get_all_categories()
    all_preview_tags = database.get_all_preview_tags()
    all_download_tags = database.get_all_download_tags()
    all_preview_kinds = database.get_all_preview_kinds()
    all_previews = database.get_all_previews()
    all_preview_preview_tags = database.get_all_preview_preview_tags()
    all_asset_tags = database.get_all_asset_tags()
    all_asset_previews = database.get_all_asset_previews()
    data = [json.loads(line) for line in open(global_data["data_path"], "r")]
    report_data = {
        "new_file_version": [],
        "new_preview_image": [],
        "new_asset": [],
        "changed_category": [],
        "updated_asset": [],
        "edited_asset": [],
    }
    for d in track(data[0], description=f"Substance assets ", total=len(data[0])):
        count = count + 1
        # console.print(d)
        # checking attached data first
        current_previews = []
        current_downloads = []
        for a in d["attachments"]:
            if a["__typename"] == "PreviewAttachment":
                preview_data = []
                for item in all_previews:
                    if item["original_id"] == a["id"]:
                        preview_data.append(item)
                        break
                if len(preview_data) == 0:
                    preview_data.append({})
                    preview_data[0]["original_id"] = a["id"]
                    preview_data[0]["url"] = a["url"]
                    preview_data[0]["label"] = a["label"]
                    preview_kind_id = -1
                    for pk in all_preview_kinds:
                        if pk["name"] == a["kind"]:
                            preview_kind_id = pk["preview_kind_id"]
                            break
                    if preview_kind_id == -1:
                        preview_kind_id = database.set_new_preview_kind(a["kind"])
                        all_preview_kinds.append(
                            {"preview_kind_id": preview_kind_id, "name": a["kind"]}
                        )
                    preview_data[0]["preview_kind_id"] = preview_kind_id
                    new_preview_id = database.set_new_preview(preview_data[0])
                    preview_data[0]["preview_id"] = new_preview_id
                    all_previews.append(preview_data[0])
                    current_previews.append(preview_data[0])
                else:
                    current_previews.append(preview_data[0])

                for t in a["tags"]:
                    tag_id = -1
                    for at in all_preview_tags:
                        if at["name"] == t:
                            tag_id = at["preview_tag_id"]
                            break
                    if tag_id == -1:
                        tag_id = database.set_new_preview_tag(t)
                        all_preview_tags.append({"preview_tag_id": tag_id, "name": t})
                    preview_preview_tag = []
                    for item in all_preview_preview_tags:
                        if (
                            item["preview_id"] == preview_data[0]["preview_id"]
                            and item["preview_tag_id"] == tag_id
                        ):
                            preview_preview_tag.append(item)
                            break
                    if len(preview_preview_tag) == 0:
                        new_preview_preview_tag_id = database.set_preview_preview_tag(
                            preview_data[0]["preview_id"], tag_id
                        )
                        all_preview_preview_tags.append(
                            {
                                "preview_preview_tag_id": new_preview_preview_tag_id,
                                "preview_id": preview_data[0]["preview_id"],
                                "preview_tag_id": tag_id,
                            }
                        )

            elif a["__typename"] == "DownloadAttachment":
                # Processing downloadable file
                download_data = database.get_download_by_original_id(a["id"])
                if len(download_data) == 0:
                    download_data.append({})
                    download_data[0]["original_id"] = a["id"]
                    download_data[0]["url"] = a["url"]
                    download_data[0]["label"] = a["label"]
                    new_download_id = database.set_new_download(download_data[0])
                    download_data[0]["download_id"] = new_download_id
                    current_downloads.append(download_data[0])
                else:
                    current_downloads.append(download_data[0])

                for t in a["tags"]:
                    tag_id = -1
                    for at in all_download_tags:
                        if at["name"] == t:
                            tag_id = at["download_tag_id"]
                            break
                    if tag_id == -1:
                        tag_id = database.set_new_download_tag(t)
                        all_download_tags.append({"download_tag_id": tag_id, "name": t})
                    download_download_tag = database.get_download_download_tag_by_download_id_and_download_tag_id(
                        download_data[0]["download_id"], tag_id
                    )
                    if len(download_download_tag) == 0:
                        database.set_download_download_tag(
                            download_data[0]["download_id"], tag_id
                        )

                for r in a["revisions"]:
                    revision_data = database.get_revisions_by_download_id_and_revision(
                        download_data[0]["download_id"], r["revision"]
                    )
                    revision_count = len(revision_data)
                    need_double = True
                    for rd in revision_data:
                        if (
                            rd["filename"] == r["filename"]
                            and rd["size"] == r["size"]
                            # or rd["created_at"] == r["createdAt"]
                        ):
                            need_double = False
                            break
                    if len(revision_data) == 0 or need_double:
                        revision_data = [{}]
                        revision_data[0]["download_id"] = download_data[0][
                            "download_id"
                        ]
                        revision_data[0]["filename"] = r["filename"]
                        revision_data[0]["size"] = r["size"]
                        revision_data[0]["revision"] = r["revision"]
                        revision_data[0]["created_at"] = r["createdAt"]
                        revision_data[0]["have_file"] = False
                        new_revision_id = database.set_new_revision(revision_data[0])
                        if revision_count > 0:
                            report_data["new_file_version"].append(
                                {
                                    "Asset": d["title"],
                                    "filename": r["filename"],
                                    "revision": r["revision"],
                                    "category": d["categories"][0],
                                }
                            )
            else:
                console.print(f"Found Unknown Attachment type - {a['__typename']} !!!")
        # and now main asset data
        asset_data = database.get_latest_asset_revision_by_original_id(d["id"])
        if len(asset_data) == 0:
            # Asset with this ID is not in the database
            asset_data.append({})
            asset_data[0]["original_id"] = d["id"]
            asset_data[0]["name"] = d["title"]
            type_id = -1
            for t in all_types:
                if t["name"] == d["__typename"]:
                    type_id = t["type_id"]
                    break
            if type_id == -1:
                type_id = database.set_new_type(d["__typename"])
                all_types.append({"type_id": type_id, "name": d["__typename"]})
            asset_data[0]["type_id"] = type_id
            asset_data[0]["is_new"] = d["new"]
            asset_data[0]["is_update"] = d["downloadsRecentlyUpdated"]
            asset_data[0]["created_at"] = d["createdAt"]

            preview_id = -1
            for item in all_previews:
                if item["original_id"] == d["thumbnail"]["id"]:
                    preview_id = item["preview_id"]
                    break
            asset_data[0]["thumbnail_id"] = preview_id
            asset_data[0]["extra_data_author"] = ""
            asset_data[0]["extra_data_physical_size"] = ""
            asset_data[0]["extra_data_ref"] = ""
            asset_data[0]["extra_data_type"] = ""
            asset_data[0]["extra_data_style"] = ""
            asset_data[0]["extra_data_quality"] = ""
            asset_data[0]["extra_data_meshes"] = ""
            asset_data[0]["extra_data_counters_quads"] = ""
            asset_data[0]["extra_data_substance_resolution"] = ""
            asset_data[0]["extra_data_preview_disp"] = ""

            for ed in d["extraData"]:
                if ed["key"] == "author":
                    asset_data[0]["extra_data_author"] = ed["value"]
                elif ed["key"] == "physicalSize":
                    asset_data[0]["extra_data_physical_size"] = ed["value"]
                elif ed["key"] == "ref":
                    asset_data[0]["extra_data_ref"] = ed["value"]
                elif ed["key"] == "type":
                    asset_data[0]["extra_data_type"] = ed["value"]
                elif ed["key"] == "style":
                    asset_data[0]["extra_data_style"] = ed["value"]
                elif ed["key"] == "quality":
                    asset_data[0]["extra_data_quality"] = ed["value"]
                elif ed["key"] == "meshes":
                    asset_data[0]["extra_data_meshes"] = ed["value"]
                elif ed["key"] == "counters.quads":
                    asset_data[0]["extra_data_counters_quads"] = ed["value"]
                elif ed["key"] == "substance_resolution":
                    asset_data[0]["extra_data_substance_resolution"] = ed["value"]
                elif ed["key"] == "previewDisp":
                    asset_data[0]["extra_data_preview_disp"] = ed["value"]
            new_asset_id = database.set_new_asset(asset_data[0]["original_id"])
            asset_data[0]["asset_id"] = new_asset_id
            new_asset_revision_id = database.set_new_asset_revision(asset_data[0])
            report_data["new_asset"].append(
                {"Asset": d["title"], "category": d["categories"][0]}
            )
        else:
            # We have asset with this ID in the database
            have_changes = False
            have_small_change = False
            small_change = []
            big_change = []
            if asset_data[0]["name"] != d["title"]:
                have_changes = True
                big_change.append(
                    f'Title changed from "{asset_data[0]["name"]}" to "{d["title"]}"'
                )
                asset_data[0]["name"] = d["title"]
            type_id = -1
            for t in all_types:
                if t["name"] == d["__typename"]:
                    type_id = t["type_id"]
                    break
            if type_id == -1:
                type_id = database.set_new_type(d["__typename"])
                all_types.append({"type_id": type_id, "name": d["__typename"]})
            if asset_data[0]["type_id"] != type_id:
                have_changes = True
                big_change.append(
                    f'Type changed from "{database.get_types_by_type_id(asset_data[0]["type_id"])[0]["name"]}" to "{d["__typename"]}"'
                )
                asset_data[0]["type_id"] = type_id
            if asset_data[0]["is_new"] != d["new"]:
                have_small_change = True
                small_change.append(
                    f'New status changed from "{bool(asset_data[0]["is_new"])}" to "{d["new"]}"'
                )
                asset_data[0]["is_new"] = d["new"]
            if asset_data[0]["is_update"] != d["downloadsRecentlyUpdated"]:
                have_small_change = True
                small_change.append(
                    f'Is Updated status changed from "{bool(asset_data[0]["is_update"])}" to "{d["downloadsRecentlyUpdated"]}"'
                )
                asset_data[0]["is_update"] = d["downloadsRecentlyUpdated"]
            if asset_data[0]["created_at"] != d["createdAt"]:
                have_changes = True
                big_change.append(
                    f'Created date changed from "{asset_data[0]["created_at"]}" to "{d["createdAt"]}"'
                )
                asset_data[0]["created_at"] = d["createdAt"]
            thumbnail_id = -1
            for item in all_previews:
                if item["original_id"] == d["thumbnail"]["id"]:
                    thumbnail_id = item["preview_id"]
                    break
            if asset_data[0]["thumbnail_id"] != thumbnail_id:
                have_changes = True
                big_change.append(
                    f'Thumbnail id changed from "{asset_data[0]["thumbnail_id"]}" to "{thumbnail_id}"'
                )
                asset_data[0]["thumbnail_id"] = thumbnail_id
                report_data["new_preview_image"].append(
                    {"Asset": d["title"], "category": d["categories"][0]}
                )

            for ed in d["extraData"]:
                if ed["key"] == "author":
                    if asset_data[0]["extra_data_author"] != ed["value"]:
                        have_small_change = True
                        small_change.append(
                            f'Extra Author changed from "{asset_data[0]["extra_data_author"]}" to "{ed["value"]}"'
                        )
                        asset_data[0]["extra_data_author"] = ed["value"]
                elif ed["key"] == "physicalSize":
                    if asset_data[0]["extra_data_physical_size"] != ed["value"]:
                        small_change.append(
                            f'Extra Physical size changed from "{asset_data[0]["extra_data_physical_size"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_physical_size"] = ed["value"]
                elif ed["key"] == "ref":
                    if asset_data[0]["extra_data_ref"] != ed["value"]:
                        have_changes = True
                        big_change.append(
                            f'Extra Internal reference changed from "{asset_data[0]["extra_data_ref"]}" to "{ed["value"]}"'
                        )
                        asset_data[0]["extra_data_ref"] = ed["value"]
                elif ed["key"] == "type":
                    if asset_data[0]["extra_data_type"] != ed["value"]:
                        small_change.append(
                            f'Extra type changed from "{asset_data[0]["extra_data_type"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_type"] = ed["value"]
                elif ed["key"] == "style":
                    if asset_data[0]["extra_data_style"] != ed["value"]:
                        small_change.append(
                            f'Extra style changed from "{asset_data[0]["extra_data_style"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_style"] = ed["value"]
                elif ed["key"] == "quality":
                    if asset_data[0]["extra_data_quality"] != ed["value"]:
                        small_change.append(
                            f'Extra quality changed from "{asset_data[0]["extra_data_quality"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_quality"] = ed["value"]
                elif ed["key"] == "meshes":
                    if asset_data[0]["extra_data_meshes"] != ed["value"]:
                        small_change.append(
                            f'Extra meshes changed from "{asset_data[0]["extra_data_meshes"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_meshes"] = ed["value"]
                elif ed["key"] == "counters.quads":
                    if asset_data[0]["extra_data_counters_quads"] != ed["value"]:
                        small_change.append(
                            f'Extra quad count changed from "{asset_data[0]["extra_data_counters_quads"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_counters_quads"] = ed["value"]
                elif ed["key"] == "substance_resolution":
                    if asset_data[0]["extra_data_substance_resolution"] != ed["value"]:
                        small_change.append(
                            f'Extra resolution changed from "{asset_data[0]["extra_data_substance_resolution"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_substance_resolution"] = ed["value"]
                elif ed["key"] == "previewDisp":
                    if asset_data[0]["extra_data_preview_disp"] != ed["value"]:
                        small_change.append(
                            f'Extra displacement changed from "{asset_data[0]["extra_data_preview_disp"]}" to "{ed["value"]}"'
                        )
                        have_small_change = True
                        asset_data[0]["extra_data_preview_disp"] = ed["value"]

            all_small_changes = ".".join(small_change)
            all_big_changes = ".".join(big_change)
            if not have_changes and have_small_change:
                database.update_asset_revision(asset_data[0])
                report_data["edited_asset"].append(
                    {
                        "Asset": d["title"],
                        "category": d["categories"][0],
                        "details": all_small_changes,
                    }
                )
            elif have_changes:
                database.update_asset_revision_revision(asset_data[0])
                report_data["updated_asset"].append(
                    {
                        "Asset": d["title"],
                        "category": d["categories"][0],
                        "details": f"{all_big_changes}. {all_small_changes}",
                    }
                )

        for t in d["tags"]:
            tag_id = -1
            for at in all_tags:
                if at["name"] == t:
                    tag_id = at["tag_id"]
                    break
            if tag_id == -1:
                tag_id = database.set_new_tag(t)
                all_tags.append({"tag_id": tag_id, "name": t})
            asset_tag = []
            for item in all_asset_tags:
                if (
                    item["asset_id"] == asset_data[0]["asset_id"]
                    and item["tag_id"] == tag_id
                ):
                    asset_tag.append(item)
                    break
            if len(asset_tag) == 0:
                new_asset_tag = database.set_asset_tag(
                    asset_data[0]["asset_id"], tag_id
                )
                all_asset_tags.append(
                    {
                        "asset_tag_id": new_asset_tag,
                        "asset_id": asset_data[0]["asset_id"],
                        "tag_id": tag_id,
                    }
                )

        all_asset_categories = database.get_asset_category_by_asset_id(
            asset_data[0]["asset_id"]
        )
        for aac in all_asset_categories:
            aac["is_active"] = False
        current_category = database.get_active_asset_category_by_asset_id(
            asset_data[0]["asset_id"]
        )
        for c in d["categories"]:
            category_id = -1
            for ac in all_categories:
                if ac["name"] == c:
                    category_id = ac["category_id"]
                    cat_data = None
                    for item in all_asset_categories:
                        if item["category_id"] == category_id:
                            cat_data = item
                            break
                    if cat_data is not None:
                        cat_data["is_active"] = True
                    break
            if category_id == -1:
                category_id = database.set_new_category(c)
                all_categories.append({"category_id": category_id, "name": c})
            asset_category = database.get_asset_category_by_asset_id_and_category_id(
                asset_data[0]["asset_id"], category_id
            )
            if len(asset_category) == 0:
                database.set_asset_category(
                    asset_data[0]["asset_id"], category_id, True
                )

        for aac in all_asset_categories:
            if not aac["is_active"]:
                database.update_asset_category(aac)
        new_category = database.get_active_asset_category_by_asset_id(
            asset_data[0]["asset_id"]
        )
        # We always assume, that is only 1 active category
        if (
            len(current_category) > 0
            and len(new_category) > 0
            and current_category[0]["category_id"] != new_category[0]["category_id"]
        ):
            report_data["changed_category"].append(
                {
                    "Asset": d["title"],
                    "old_category": database.get_category_by_id(
                        current_category[0]["category_id"]
                    )[0]["name"],
                    "new_category": database.get_category_by_id(
                        new_category[0]["category_id"]
                    )[0]["name"],
                }
            )

        for cp in current_previews:
            asset_preview = []
            for item in all_asset_previews:
                if (
                    item["asset_id"] == asset_data[0]["asset_id"]
                    and item["preview_id"] == cp["preview_id"]
                ):
                    asset_preview.append(item)
                    break
            if len(asset_preview) == 0:
                new_asset_preview_id = database.set_asset_preview(
                    asset_data[0]["asset_id"], cp["preview_id"]
                )
                all_asset_previews.append(
                    {
                        "asset_preview_id": new_asset_preview_id,
                        "asset_id": asset_data[0]["asset_id"],
                        "preview_id": cp["preview_id"],
                    }
                )

        for cd in current_downloads:
            asset_download = database.get_asset_download_by_asset_id_and_download_id(
                asset_data[0]["asset_id"], cd["download_id"]
            )
            if len(asset_download) == 0:
                database.set_asset_download(
                    asset_data[0]["asset_id"], cd["download_id"]
                )

    console.print("New elements - " + str(len(report_data["new_asset"])))
    console.print("Updated elements - " + str(len(report_data["updated_asset"])))
    console.print("Edited elements - " + str(len(report_data["edited_asset"])))
    console.print("Changed category - " + str(len(report_data["changed_category"])))
    console.print("File new versions - " + str(len(report_data["new_file_version"])))
    console.print("New preview images - " + str(len(report_data["new_preview_image"])))
    console.print()
    console.print("All Done !!!")

    if (
        len(report_data["new_file_version"]) > 0
        or len(report_data["new_preview_image"]) > 0
        or len(report_data["new_asset"]) > 0
        or len(report_data["changed_category"]) > 0
        or len(report_data["updated_asset"]) > 0
        or len(report_data["edited_asset"]) > 0
    ):
        file = open(
            append_date(global_data["local_path"] + os.sep + "Scan Report.txt"),
            "w",
            encoding="utf-8",
        )
        if len(report_data["new_asset"]) > 0:
            file.write(f"New assets: {len(report_data['new_asset'])}\n\n")
            for rd in report_data["new_asset"]:
                file.write(f"{rd['category']} -- {rd['Asset']}" + "\n")
            file.write("\n")
        if len(report_data["edited_asset"]) > 0:
            file.write(
                f"Edited assets (small change): {len(report_data['edited_asset'])}\n\n"
            )
            for rd in report_data["edited_asset"]:
                file.write(
                    f"{rd['category']} -- {rd['Asset']} ({rd['details']})" + "\n"
                )
            file.write("\n")
        if len(report_data["updated_asset"]) > 0:
            file.write(
                f"Updated assets (new revision): {len(report_data['updated_asset'])}\n\n"
            )
            for rd in report_data["updated_asset"]:
                file.write(
                    f"{rd['category']} -- {rd['Asset']} ({rd['details']})" + "\n"
                )
            file.write("\n")
        if len(report_data["changed_category"]) > 0:
            file.write(f"Changed Category: {len(report_data['changed_category'])}\n\n")
            for rd in report_data["changed_category"]:
                file.write(
                    f"{rd['Asset']} -- *From* {rd['old_category']} *To* {rd['new_category']}"
                    + "\n"
                )
            file.write("\n")
        if len(report_data["new_preview_image"]) > 0:
            file.write(
                f"New preview image: {len(report_data['new_preview_image'])}\n\n"
            )
            for rd in report_data["new_preview_image"]:
                file.write(f"{rd['category']} -- {rd['Asset']}" + "\n")
            file.write("\n")
        if len(report_data["new_file_version"]) > 0:
            file.write(f"New File Versions {len(report_data['new_file_version'])}:\n\n")
            for rd in report_data["new_file_version"]:
                file.write(
                    f"{rd['category']} -- {rd['Asset']} -- {rd['filename']} -- Revision {rd['revision']}"
                    + "\n"
                )
            file.write("\n")
        file.close()

    input("Press Enter to continue... (Close App to save changes !!!)")


def main():
    """
    Main menu to  select activity
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-d",
        "--database",
        default="all_assets.db",
        help="Path to the SQLite file. (Default is %(default)s",
    )
    args = parser.parse_args()
    database = CommonDatabaseAccess(db_path=args.database, force=True)

    menu_title = " Select action"
    menu_items = [
        "[1] Scrap online data",
        "[2] Process online data",
        "[3] Quit (Close App to save changes !!!)",
    ]

    local_path = os.path.dirname(sys.argv[0])
    global_data["local_path"] = local_path
    global_data["data_path"] = f"{local_path}\\all_assets_raw.txt"
    # process_online_data(database, args.debug)
    # sys.exit(0)
    menu_exit = False
    while not menu_exit:
        clear_console()
        console.print("version " + global_data["version"])
        console.print(menu_title + "")
        console.print()

        if os.path.exists(global_data["data_path"]):
            # first check when file was created and display that
            file_time = datetime.fromtimestamp(path.getmtime(global_data["data_path"]))
            console.print(f"Data file created {get_duration(file_time)} ago.\n")
        else:
            console.print("[red]Missing data file !!!!")
        console.print()

        for m_i in menu_items:
            console.print(m_i + "")
        console.print("")
        user_input = input("Enter a number: ")
        if user_input.isnumeric():
            menu_sel = int(user_input)
            if menu_sel == 1:  # Scrap online data
                scrap_online_data()
            elif menu_sel == 2:  # Process online data
                process_online_data(database)
            elif menu_sel == 3:  # Quit
                menu_exit = True


if __name__ == "__main__":
    main()
