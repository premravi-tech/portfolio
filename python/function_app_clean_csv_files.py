import azure.functions as func
import logging
import os
import io
import re
import pandas as pd
from typing import List, Dict, Optional
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# ---------- helpers ----------

def get_secret_from_kv(vault_url: str, secret_name: str) -> str:
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value


def clean_filename(filename: str) -> str:
    """strip, remove / #, replace - . space with _, drop other specials."""
    filename = filename.strip()
    filename = filename.replace("/", "").replace("#", "")
    filename = re.sub(r"[-.\s]", "_", filename)      # -, ., spaces -> _
    filename = re.sub(r"[^A-Za-z0-9_]", "", filename)  # remove anything else
    # collapse consecutive underscores
    filename = re.sub(r"_+", "_", filename).strip("_")
    return filename


def load_mapping(blob_service: BlobServiceClient, container: str, mapping_path: str) -> List[Dict[str, str]]:
    """Read mapping csv: columns filename,schema,tablename"""
    try:
        bc = blob_service.get_blob_client(container=container, blob=mapping_path)
        raw = bc.download_blob().readall().decode("utf-8-sig")
        mdf = pd.read_csv(io.StringIO(raw))  # expects columns: filename,schema,tablename
        # normalize / clean
        mdf.columns = [c.strip().lower() for c in mdf.columns]
        req = {"filename", "schema", "tablename"}
        if not req.issubset(set(mdf.columns)):
            raise ValueError(f"Mapping file must have columns {req}, got {mdf.columns.tolist()}")
        mdf = mdf.dropna(subset=["filename"]).fillna("")
        rows = []
        for _, r in mdf.iterrows():
            rows.append({
                "filename": str(r["filename"]).strip(),
                "schema": str(r["schema"]).strip(),
                "tablename": str(r["tablename"]).strip(),
            })
        return rows
    except Exception as e:
        logging.warning(f"Mapping file not loaded ({mapping_path}). Proceeding without mapping. Reason: {e}")
        return []


def map_output_name(base_filename: str, mappings: List[Dict[str, str]]) -> Optional[str]:
    """Return mapped name like schema.tablename.csv if base_filename startswith any mapping.filename"""
    lower_base = base_filename.lower()
    for m in mappings:
        key = m["filename"].lower()
        if key and lower_base.startswith(key):
            schema = clean_filename(m["schema"]) if m["schema"] else ""
            tablename = clean_filename(m["tablename"]) if m["tablename"] else ""
            if schema and tablename:
                return f"{tablename}.csv"
            elif tablename:
                return f"{tablename}.csv"
            # if mapping row exists but missing names, fall through to default clean
    return None


# ---------- function ----------

@app.route(route="process_csv")
def process_csv(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("CSV processing function triggered.")

    try:
        # env
        keyvault_url = os.getenv("KEYVAULT_URL")
        storage_account = os.getenv("STORAGE_ACCOUNT_NAME")
        storage_key_secret_name = os.getenv("STORAGE_KEY_SECRET_NAME")

        source_container = os.getenv("SOURCE_CONTAINER")
        dest_container = os.getenv("DEST_CONTAINER")
        source_path = (os.getenv("SOURCE_PATH") or "").strip("/")
        dest_path = (os.getenv("DEST_PATH") or "").strip("/")

        # mapping config
        mapping_path = os.getenv("MAPPING_PATH", "Dropbox_TableNames/file_to_table_name_mapping.csv")

        # kv -> storage key
        storage_key = get_secret_from_kv(keyvault_url, storage_key_secret_name)

        # blob clients
        bsc = BlobServiceClient(f"https://{storage_account}.blob.core.windows.net", credential=storage_key)
        src_cc = bsc.get_container_client(source_container)
        dst_cc = bsc.get_container_client(dest_container)

        # prefixes
        src_prefix = f"{source_path}/" if source_path else ""
        dst_prefix = f"{dest_path}/" if dest_path else ""

        # load mapping
        mappings = load_mapping(bsc, dest_container, mapping_path)  # mapping file sits in same container
        if mappings:
            logging.info(f"Loaded {len(mappings)} mapping rows from {mapping_path}")
        else:
            logging.info("No mapping rows loaded; will use cleaned filenames.")

        processed = 0
        failed: List[str] = []

        for blob in src_cc.list_blobs(name_starts_with=src_prefix):
            if not blob.name.lower().endswith(".csv"):
                continue

            base = os.path.basename(blob.name)
            # determine output file name
            mapped_name = map_output_name(base, mappings)
            if mapped_name:
                out_name = mapped_name
                logging.info(f"Mapping applied: {base} -> {out_name}")
            else:
                out_name = f"{clean_filename(os.path.splitext(base)[0])}.csv"
                logging.info(f"No mapping; cleaned: {base} -> {out_name}")

            try:
                # download
                csv_bytes = src_cc.download_blob(blob.name).readall()
                csv_text = csv_bytes.decode("utf-8-sig")

                # parse with multiline
                df = pd.read_csv(
                    io.StringIO(csv_text),
                    quotechar='"',
                    escapechar='\\',
                    engine='python'
                )

                # header & dtype cleanup
                df.columns = [clean_filename(c) for c in df.columns]
                df = df.fillna("").astype(str)
                # write
                buff = io.StringIO()
                df.to_csv(buff, index=False)
                dest_blob_name = f"{dst_prefix}{out_name}"
                dst_cc.upload_blob(name=dest_blob_name, data=buff.getvalue(), overwrite=True)

                logging.info(f"Saved: {dest_blob_name}")
                processed += 1

            except Exception as fe:
                logging.error(f"Failed {blob.name}: {fe}", exc_info=True)
                failed.append(blob.name)

        if failed:
            return func.HttpResponse(
                f"Processed {processed} files, failed {len(failed)}: {failed[:5]}{' ...' if len(failed)>5 else ''}",
                status_code=500
            )

        return func.HttpResponse(f"Processed {processed} CSV files successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
