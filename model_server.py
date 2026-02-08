from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from uuid import uuid4

# ====== 目录初始化 ======
MODEL_ROOT = Path("models")
MODEL_ROOT.mkdir(exist_ok=True)

RAW_DIR = MODEL_ROOT / "raw"
RAW_DIR.mkdir(exist_ok=True)

# ====== FastAPI init ======
app = FastAPI(title="Simple Model Library")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 前端用 /files/... 访问模型文件
app.mount("/files", StaticFiles(directory=str(MODEL_ROOT)), name="files")

# ====== ping ======
@app.get("/ping")
def ping():
    return {"ok": True, "service": "simple-model-library"}


# ====== 上传文件 ======
@app.post("/raw/upload")
async def raw_upload(file: UploadFile = File(...)):
    """上传文件到 models/raw/，返回可下载的 file_url"""
    if not file.filename:
        raise HTTPException(400, "Empty filename")

    suffix = Path(file.filename).suffix or ""
    model_id = uuid4().hex
    stored_name = model_id + suffix
    fpath = RAW_DIR / stored_name

    content = await file.read()
    with open(fpath, "wb") as f:
        f.write(content)

    file_url = f"/files/raw/{stored_name}"

    return {
        "ok": True,
        "model_id": model_id,
        "original_name": file.filename,
        "file_url": file_url,
        "file_size": len(content),
    }


# ====== 列出文件 ======
@app.get("/raw/list")
def raw_list():
    """列出 models/raw 下所有文件，包含下载用的 file_url"""
    files = []
    for p in sorted(RAW_DIR.glob("*")):
        if p.is_file():
            rel = p.relative_to(MODEL_ROOT)  # raw/xxxxx.glb
            files.append({
                "name": p.name,                         # 文件名
                "file_url": f"/files/{rel.as_posix()}", # 前端可访问的 URL
                "size": p.stat().st_size,
            })
    return files


# ====== 删除文件（重要） ======
@app.delete("/raw/delete/{filename}")
def delete_raw_file(filename: str):
    """
    通过文件名删除 models/raw/ 下的文件
    """
    target = RAW_DIR / filename

    # 检查文件是否存在
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    # 删除文件
    target.unlink()

    return {"ok": True, "deleted": filename}
