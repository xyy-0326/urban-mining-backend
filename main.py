# main.py
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# --- Neo4j config ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
if not NEO4J_PASSWORD:
    raise RuntimeError("NEO4J_PASSWORD is not set")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://urbanmining-kassel.vercel.app"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------- helpers ----------
def _run_list(cypher: str, **params) -> List[Dict[str, Any]]:
    try:
        with driver.session() as s:
            return [r.data() for r in s.run(cypher, **params)]
    except Neo4jError as e:
        raise HTTPException(status_code=503, detail=f"Neo4jError: {e.code}| {e.message}")


def _run_single(cypher: str, **params) -> Optional[Dict[str, Any]]:
    try:
        with driver.session() as s:
            rec = s.run(cypher, **params).single()
            return rec.data() if rec else None
    except Neo4jError as e:
        raise HTTPException(status_code=503, detail=f"Neo4jError: {e.code}| {e.message}")


def _parse_fields(s: Optional[str]) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _pick_props(props: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    return {k: props.get(k) for k in fields if k in props} if fields else {}


# ---------- endpoints ----------
@app.get("/ping")
def ping():
    # 不查 Neo4j：只要 FastAPI 存活就 ok
    return {"status": "ok"}


@app.get("/linked_osm_ids")
def linked_osm_ids():
    cypher = """
    MATCH (n)
    WHERE (n:dice_Building OR n:dice_BuildingUnit) AND n.osm_id IS NOT NULL
    RETURN DISTINCT n.osm_id AS osm_id
    """
    rows = _run_list(cypher)
    return {"osm_ids": [r["osm_id"] for r in rows if r.get("osm_id")]}


@app.get("/building")
def get_building(osm_id: str):
    cypher = """
    MATCH (n {osm_id: $osm_id})
    WHERE n:dice_Building OR n:dice_BuildingUnit

    OPTIONAL MATCH (n)<-[:dicer_hasPart]-(parent:dice_Building)
    WITH n, parent, coalesce(parent, n) AS mainBuilding

    OPTIONAL MATCH (mainBuilding)-[:hasProperty]->(bc:dicv_Property)
    WHERE bc.name STARTS WITH "buildingClass"
    WITH n, parent, mainBuilding, head(collect(DISTINCT bc.name)) AS bcName

    OPTIONAL MATCH (mainBuilding)-[:hasRole]->(r:dice_Role)
    WITH n, parent, mainBuilding, bcName, head(collect(DISTINCT r.name)) AS roleName

    // structural system from Category(Type="structural_system")
    OPTIONAL MATCH (mainBuilding)-[:isClassifiedBy]->(cat:Category)
    WHERE cat.Type = "structural_system" AND cat.name IS NOT NULL
    WITH n, parent, mainBuilding, bcName, roleName,
         head(collect(DISTINCT cat.name)) AS structuralSystem

    OPTIONAL MATCH (mainBuilding)-[:hasLocation]->(loc)
    OPTIONAL MATCH (mainBuilding)-[:regulatedBy]->(plan:PlanningDocument)
    OPTIONAL MATCH (mainBuilding)-[:inDistrict]->(dist:District)
    WITH n, parent, mainBuilding, loc, plan, bcName, roleName, structuralSystem,
         collect(DISTINCT dist.name) AS districtNames

    OPTIONAL MATCH (mainBuilding)-[:dicer_hasPart]->(bo:dice_BuildingObject)
    WITH n, parent, mainBuilding, loc, plan, bcName, roleName, structuralSystem, districtNames,
         collect(DISTINCT bo) AS bos

    WITH
      n, parent, mainBuilding, loc, plan, bcName, roleName, structuralSystem, districtNames,
      [bo IN bos |
        {
          name:      coalesce(bo.name, bo.id, bo.uuid),
          number:    coalesce(bo.number, 0),
          materials: [(bo)-[:hasMaterial]->(mm:dice_MaterialEntity) | coalesce(mm.name, mm.id, mm.uuid)],
          width_mm:  bo.width_mm,
          height_mm: bo.height_mm,
          length_mm: bo.length_mm
        }
      ] AS componentInfo

    WITH
      n, parent, mainBuilding, loc, plan, bcName, roleName, structuralSystem, districtNames, componentInfo,
      [ci IN componentInfo | ci.name] AS components,
      reduce(xs = [], ci IN componentInfo | xs + ci.materials) AS materialsRaw

    RETURN {
      found: true,
      propsMain: properties(mainBuilding),
      propsPart: CASE WHEN parent IS NULL OR parent = n THEN null ELSE properties(n) END,
      bcName: bcName,
      roleName: roleName,
      structuralSystem: structuralSystem,
      location: properties(loc),
      planning: CASE WHEN plan IS NULL THEN null ELSE {
        title: plan.title, url: plan.url, planNo: plan.planNo, lastModified: plan.lastModified
      } END,
      materials: materialsRaw,
      components: components,
      componentInfo: componentInfo,
      district: CASE WHEN size(districtNames) > 0 THEN districtNames[0] ELSE null END
    } AS result
    """

    rec = _run_single(cypher, osm_id=osm_id)
    if not rec or not rec.get("result") or not rec["result"].get("propsMain"):
        return {"found": False}

    r = rec["result"]
    props = dict(r.get("propsMain") or {})

    # normalize buildingClass into int, based on property name "buildingClass3"/"buildingClass5"
    bc_name = (r.get("bcName") or "").strip()
    if bc_name.startswith("buildingClass"):
        tail = bc_name.replace("buildingClass", "").strip()
        if tail.isdigit():
            props["buildingClass"] = int(tail)
        else:
            props["buildingClass"] = bc_name  # fallback

    # buildingType from Role
    if r.get("roleName"):
        props["buildingType"] = r["roleName"]

    # structural system from Category(Type="structural_system")
    if r.get("structuralSystem"):
        props["structuralSystem"] = r["structuralSystem"]

    return {
        "found": True,
        "properties": props,
        "partProperties": r.get("propsPart"),
        "location": r.get("location"),
        "planning": r.get("planning"),
        "materials": r.get("materials") or [],
        "components": r.get("components") or [],
        "componentInfo": r.get("componentInfo") or [],
        "district": r.get("district"),
    }


@app.get("/component-info")
def component_info(
    name: str,
    cat_fields: Optional[str] = Query(default=None, description="Comma-separated Category fields to return"),
):
    fields = _parse_fields(cat_fields)

    cypher = """
    MATCH (b)-[:dicer_hasPart]->(bo:dice_BuildingObject)
    WHERE (b:dice_Building OR b:dice_BuildingUnit)
      AND coalesce(bo.name, bo.id, bo.uuid) = $name

    OPTIONAL MATCH (b)<-[:dicer_hasPart]-(parent:dice_Building)
    WITH bo, coalesce(parent, b) AS mainBuilding

    OPTIONAL MATCH (bo)-[:hasMaterial]->(m:dice_MaterialEntity)
    OPTIONAL MATCH (bo)-[:isClassifiedBy|hasCategory|dicer_isClassifiedBy]-(c:Category)
    WITH mainBuilding, bo, collect(DISTINCT m) AS ms, collect(DISTINCT c) AS cs

    RETURN
      coalesce(mainBuilding.name, mainBuilding.buildingName, mainBuilding.osm_name) AS building,
      mainBuilding.building_id AS building_id,
      mainBuilding.osm_id AS osm_id,
      coalesce(bo.number, bo.properties.count, 0) AS number,
      bo.width_mm  AS width_mm,
      bo.length_mm AS length_mm,
      bo.height_mm AS height_mm,
      [mm IN ms WHERE mm IS NOT NULL | coalesce(mm.name, mm.id, mm.uuid)] AS materials,
      [cc IN cs WHERE cc IS NOT NULL | cc.name] AS categories,
      [cc IN cs WHERE cc IS NOT NULL | {name: cc.name, props: properties(cc)}] AS categoryPropsRaw
    ORDER BY building
    """

    rows = _run_list(cypher, name=name)
    out: List[Dict[str, Any]] = []

    for r in rows:
        cat_props: List[Dict[str, Any]] = []
        for item in (r.get("categoryPropsRaw") or []):
            if not item or not item.get("name"):
                continue
            payload = {"name": item["name"]}
            payload.update(_pick_props(item.get("props") or {}, fields))
            cat_props.append(payload)

        out.append(
            {
                "building": r.get("building"),
                "building_id": r.get("building_id"),
                "osm_id": r.get("osm_id"),
                "number": r.get("number") or 0,
                "materials": r.get("materials") or [],
                "categories": r.get("categories") or [],
                "categoryProps": cat_props if fields else [],
                "width_mm": r.get("width_mm"),
                "length_mm": r.get("length_mm"),
                "height_mm": r.get("height_mm"),
            }
        )

    return out


@app.get("/material-volume-all")
def material_volume_all():
    cypher = """
    MATCH (m:dice_MaterialEntity)-[:hasQuantitativeProperty]->(q:dicv_QuantitativeProperty)
    WHERE q.type = 'Volume' AND q.value_m3 IS NOT NULL
    RETURN coalesce(m.name, q.material_id) AS material, sum(q.value_m3) AS volume_m3
    ORDER BY volume_m3 DESC
    """
    rows = _run_list(cypher)
    return [{"material": r["material"], "volume_m3": r["volume_m3"]} for r in rows]


@app.get("/material-volume-building")
def material_volume_building(building_id: str):
    cypher = """
    MATCH (m:dice_MaterialEntity)-[:hasQuantitativeProperty]->(q:dicv_QuantitativeProperty)
    WHERE q.type = 'Volume' AND q.building_id = $building_id AND q.value_m3 IS NOT NULL
    RETURN coalesce(m.name, q.material_id) AS material, sum(q.value_m3) AS volume_m3
    ORDER BY volume_m3 DESC
    """
    rows = _run_list(cypher, building_id=building_id)
    return [{"material": r["material"], "volume_m3": r["volume_m3"]} for r in rows]
