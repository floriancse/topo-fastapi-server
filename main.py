from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()  # charge .env en local

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode": os.getenv("DB_SSLMODE", "require"),
}

app = FastAPI()

# CORS allowed origins
ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5500"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fetch_elevation_data(wkt: str) -> list:
    """Retrieve elevation data and coordinates from PostGIS for a given WKT linestring."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    CAST(
                        ST_VALUE (R.RAST, ST_TRANSFORM (D.GEOM, 4326)) AS INT
                    ) AS VALUE,
                    ST_ASGEOJSON (D.GEOM) AS COORDS
                FROM
                    GEO_BDR_MNT  R,
                    ST_DUMPPOINTS (
                        ST_LINEINTERPOLATEPOINTS (ST_GEOMFROMTEXT (%s, 3857), 0.01)
                    ) AS D (PATH, GEOM)
                WHERE
                    ST_INTERSECTS (R.RAST, ST_TRANSFORM (D.GEOM, 4326));
                """,
                (wkt,),
            )
            return cursor.fetchall()


def calculate_linestring_length(wkt: str) -> float:
    """Calculate the length of a WKT linestring using PostGIS."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    public.ST_LENGTH (public.ST_GEOMFROMTEXT (%s::text, 3857));
                """,
                (wkt,),
            )
            return cursor.fetchall()[0][0]


def fetch_hydrographic_info(wkt: str) -> list:
    """Retrieve hydrological information intersecting with a WKT linestring."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH
                    TOPO_LINE AS (
                        SELECT
                            ST_GEOMFROMTEXT (
                                %s,
                                3857
                            ) AS GEOM
                    )
                SELECT
                    C.NOMENTITEH,
                    C.CLASSE,
                    (
                        ST_LINELOCATEPOINT (L.GEOM, (D).GEOM) * ST_LENGTH (L.GEOM)
                    )::INT AS DIST_M
                FROM
                    GEO_COURS_EAU AS C
                    JOIN TOPO_LINE AS L ON ST_CROSSES (C.GEOM, L.GEOM)
                    CROSS JOIN LATERAL ST_DUMP (ST_INTERSECTION (C.GEOM, L.GEOM)) AS D
                WHERE
                    C.CLASSE IN (1, 2, 3)
                ORDER BY
                    DIST_M;
                """,
                (wkt,),
            )
            return cursor.fetchall()


def fetch_clc_data(wkt: str) -> list:
    """Retrieve Corine Land Cover (CLC) information for a WKT linestring."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH
                    TOPO_LINE AS (
                        SELECT
                            ST_GEOMFROMTEXT(%s, 3857) AS GEOM
                    )
                SELECT
                    CODE_CLC,
                    DESCRIPTION_CLC,
                    SUM(ST_LENGTH (SEG.GEOM)) OVER (
                        ORDER BY
                            POSITION ROWS BETWEEN UNBOUNDED PRECEDING
                            AND CURRENT ROW
                    )::int AS LONGUEUR_CUMULEE
                FROM
                    (
                        SELECT
                            C.CODE_CLC,
                            C.DESCRIPTION_CLC,
                            (ST_DUMP (ST_INTERSECTION (L.GEOM, C.GEOM))).GEOM AS GEOM,
                            ST_LINELOCATEPOINT (
                                L.GEOM,
                                ST_CENTROID ((ST_DUMP (ST_INTERSECTION (L.GEOM, C.GEOM))).GEOM)
                            ) AS POSITION
                        FROM
                            GEO_CLC_2018 AS C
                            JOIN TOPO_LINE AS L ON ST_INTERSECTS (L.GEOM, C.GEOM)
                    ) AS SEG
                WHERE
                    ST_GEOMETRYTYPE (SEG.GEOM) LIKE 'ST_LineString%%'
                ORDER BY
                    POSITION;
                """,
                (wkt,),
            )
            return cursor.fetchall()


def calculate_clc_percentage(wkt: str) -> list:
    """Calculate percentage of Corine Land Cover types along a WKT linestring."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH
                    TOPO_LINE AS (
                        SELECT
                            ST_GEOMFROMTEXT(%s, 3857) AS GEOM
                    )
                SELECT
                    C.CODE_CLC,
                    C.DESCRIPTION_CLC,
                    SUM(
                        ST_LENGTH (ST_INTERSECTION (L.GEOM, C.GEOM)) / ST_LENGTH (L.GEOM) * 100
                    )::INT AS LONGUEUR_PRCT
                FROM
                    GEO_CLC_2018 AS C
                    JOIN TOPO_LINE AS L ON ST_INTERSECTS (L.GEOM, C.GEOM)
                GROUP BY
                    C.CODE_CLC,
                    C.DESCRIPTION_CLC
                ORDER BY
                    LONGUEUR_PRCT DESC
                """,
                (wkt,),
            )
            return cursor.fetchall()


@app.post("/GeoProfile")
async def process_linestring(request: Request) -> dict:
    data = await request.json()
    wkt = f"LINESTRING({', '.join(f'{coord[0]} {coord[1]}' for coord in data['coords'])})"

    total_length = calculate_linestring_length(wkt)
    elevation_points = [
        [point[0] if point[0] is not None else 0, point[1]]
        for point in fetch_elevation_data(wkt)
    ]
    segment_length = total_length / len(elevation_points)
    geospatial_data = [
        {
            "distance": int(segment_length * index),
            "elevation": point[0],
            "geometry": json.loads(point[1])["coordinates"],
        }
        for index, point in enumerate(elevation_points)
    ]
    return {
        "values": geospatial_data,
        "hydro_info": fetch_hydrographic_info(wkt),
        "clc_info": fetch_clc_data(wkt),
        "clc_pct_info": calculate_clc_percentage(wkt),
    }