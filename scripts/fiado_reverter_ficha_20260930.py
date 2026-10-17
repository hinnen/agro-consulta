"""Devolve as 20 fichas de fiado para onde estavam em 30/09/2026. Não mexe em valor."""
from pathlib import Path

import psycopg

ENV = Path(__file__).resolve().parents[1] / ".env"
URL = ""
for line in ENV.read_text(encoding="utf-8").splitlines():
    if line.startswith("AGRO_CATALOGO_DEST_DATABASE_URL="):
        URL = line.split("=", 1)[1].strip().strip('"').strip("'")
        break
if not URL or "dpg-d70n26h5pdvs739beahg-a" not in URL:
    raise SystemExit("banco da loja nao encontrado")
if "sslmode=" not in URL:
    URL += ("&" if "?" in URL else "?") + "sslmode=require"

IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 20, 136, 137, 138, 139, 140]


def dinheiro(cur):
    cur.execute(
        """
        SELECT COUNT(*), ROUND(COALESCE(SUM(valor_bruto - valor_pago), 0), 2)
        FROM produtos_fiadotituloagro
        WHERE situacao NOT IN ('quitado', 'cancelado')
          AND valor_bruto > valor_pago
        """
    )
    return cur.fetchone()


def main():
    with psycopg.connect(URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if cur.fetchone()[0] != "agro_db_o9rr":
                raise SystemExit("banco errado")
            n_antes, total_antes = dinheiro(cur)
            cur.execute(
                """
                UPDATE produtos_fiadotituloagro AS t
                SET cliente_agro_id = (t.dados_snapshot_json #>> '{vinculo_ficha_20260930,antes_cliente_agro_id}')::int,
                    dados_snapshot_json = t.dados_snapshot_json - 'vinculo_ficha_20260930',
                    atualizado_em = NOW()
                WHERE t.id = ANY(%s)
                  AND t.cliente_agro_id = (t.dados_snapshot_json #>> '{vinculo_ficha_20260930,depois_cliente_agro_id}')::int
                  AND (t.dados_snapshot_json #>> '{vinculo_ficha_20260930,antes_cliente_agro_id}') ~ '^[0-9]+$'
                """,
                (IDS,),
            )
            if cur.rowcount != 20:
                raise SystemExit(f"devolveu {cur.rowcount}, esperado 20")
            n_depois, total_depois = dinheiro(cur)
            if n_depois != n_antes or total_depois != total_antes:
                raise SystemExit(f"total mudou {total_antes} -> {total_depois}")
        conn.commit()
    print(f"voltou titulos={n_depois} total={total_depois}")


if __name__ == "__main__":
    main()
