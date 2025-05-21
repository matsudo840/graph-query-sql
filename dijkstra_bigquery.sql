-- 例題5: 無理やりダイクストラ
-- このクエリは、BigQueryの再帰CTEを使用してダイクストラ法を模倣し、
-- 指定された始点から終点への最短経路とそのコストを計算します。
-- ダイクストラ法の主要なステップ（コスト最小の未確定ノードを選択し、そこからコストを更新）を
-- SQLの配列操作と再帰処理でシミュレートしています。

WITH RECURSIVE
  -- ステップ1: グラフの接続情報（エッジとコスト）を定義します。
  -- edges_input: 元の単方向エッジのリスト。
  -- edges: edges_inputから双方向のエッジを生成し、無向グラフとして扱えるようにします。
  edges_input AS (
    SELECT 'A' AS node1, 'B' AS node2, 3 AS cost UNION ALL
    SELECT 'A', 'C', 4 UNION ALL
    SELECT 'B', 'D', 2 UNION ALL
    SELECT 'B', 'E', 6 UNION ALL
    SELECT 'C', 'E', 1 UNION ALL
    SELECT 'D', 'E', 3 UNION ALL
    SELECT 'D', 'F', 7 UNION ALL
    SELECT 'E', 'F', 6
  ),
  edges AS (
    SELECT node1 AS source_node, node2 AS target_node, cost FROM edges_input
    UNION ALL
    SELECT node2 AS source_node, node1 AS target_node, cost FROM edges_input -- 逆方向のエッジも追加
  ),
  -- グラフ内の総ノード数を計算します。これは再帰処理の最大反復回数の目安として使用します。
  node_count AS (
    SELECT COUNT(DISTINCT node) AS cnt
    FROM (SELECT source_node AS node FROM edges UNION DISTINCT SELECT target_node AS node FROM edges)
  ),

  -- ダイクストラ法の反復処理を行う再帰CTE
  -- iteration: 現在の反復（ステップ）番号
  -- settled_nodes: 確定済みノードの配列。各要素は {node, cost, path} のSTRUCT。
  -- unsettled_nodes: 未確定ノードの配列。各要素は {node, cost, path} のSTRUCT。
  dijkstra_iterations AS (
    -- 初期状態: ダイクストラ法の開始時
    SELECT
      0 AS iteration, -- 開始イテレーションは0
      CAST([] AS ARRAY<STRUCT<node STRING, cost INT64, path ARRAY<STRING>>>) AS settled_nodes, -- 最初は確定ノードなし
      [STRUCT('A' AS node, 0 AS cost, ['A'] AS path)] AS unsettled_nodes -- 始点ノード'A'をコスト0で未確定リストに追加

    UNION ALL

    -- 再帰ステップ: 各イテレーションでの処理
    SELECT
      prev.iteration + 1 AS iteration, -- イテレーション番号をインクリメント

      -- settled_nodes の更新:
      -- ダイクストラ法の「未確定ノードの中からコスト最小のノードを選び、確定する」ステップに相当。
      ARRAY_CONCAT(
        prev.settled_nodes, -- 前回までの確定ノードリスト
        -- 今回新たに確定するノード (コスト最小の未確定ノード)
        (SELECT ARRAY(
          SELECT AS STRUCT nbs.node, nbs.cost, nbs.path
          FROM UNNEST(prev.unsettled_nodes) AS nbs ORDER BY nbs.cost ASC, ARRAY_TO_STRING(nbs.path, '->') ASC LIMIT 1
        ))
      ) AS settled_nodes,

      -- unsettled_nodes の更新:
      -- ダイクストラ法の「今回確定したノードから繋がる未確定ノードのコストを更新する」ステップに相当。
      (
        SELECT ARRAY_AGG(STRUCT(ranked.node, ranked.cost, ranked.path) ORDER BY ranked.cost ASC, ARRAY_TO_STRING(ranked.path, '->') ASC)
        FROM ( -- ranked_candidates: 各未確定ノード候補について、最小コストのパスのみを選択するためのランク付け
            SELECT
                all_cand.node, all_cand.cost, all_cand.path,
                ROW_NUMBER() OVER (PARTITION BY all_cand.node ORDER BY all_cand.cost ASC, ARRAY_TO_STRING(all_cand.path, '->') ASC) as rn
            FROM ( -- all_candidates_for_unsettled: 更新後の未確定ノード候補の全リスト
                -- remaining_unsettled: 今回確定したノード以外の、既存の未確定ノード
                SELECT candidate.node, candidate.cost, candidate.path
                FROM
                  UNNEST(prev.unsettled_nodes) AS candidate
                  CROSS JOIN ( -- nbs_info: 今回確定したノードの情報 (node_being_settled_struct相当)
                    SELECT nbs_calc.node AS nbs_node, nbs_calc.cost AS nbs_cost, nbs_calc.path AS nbs_path
                    FROM UNNEST(prev.unsettled_nodes) AS nbs_calc ORDER BY nbs_calc.cost ASC, ARRAY_TO_STRING(nbs_calc.path, '->') ASC LIMIT 1
                  ) AS nbs_info
                WHERE candidate.node != nbs_info.nbs_node -- 今回確定したノードは除外

                UNION ALL

                -- expanded_from_settled: 今回確定したノードを経由して到達可能な隣接ノード (コスト更新候補)
                SELECT
                  e.target_node AS node,
                  nbs_info.nbs_cost + e.cost AS cost, -- 新しいコスト = 今回確定ノードのコスト + エッジのコスト
                  ARRAY_CONCAT(nbs_info.nbs_path, [e.target_node]) AS path -- 新しいパス
                FROM
                  edges AS e
                  CROSS JOIN ( -- nbs_info: 今回確定したノードの情報
                    SELECT nbs_calc.node AS nbs_node, nbs_calc.cost AS nbs_cost, nbs_calc.path AS nbs_path
                    FROM UNNEST(prev.unsettled_nodes) AS nbs_calc ORDER BY nbs_calc.cost ASC, ARRAY_TO_STRING(nbs_calc.path, '->') ASC LIMIT 1
                  ) AS nbs_info
                WHERE
                  e.source_node = nbs_info.nbs_node -- 今回確定したノードから出るエッジ
                  AND nbs_info.nbs_node IS NOT NULL -- nbs_infoが空でないことを保証 (通常unsettled_nodesが空ならループ終了)
                  AND NOT EXISTS ( -- 既に完全に確定済みのノードは、コスト更新の対象外
                    SELECT 1
                    FROM UNNEST(
                      -- 現時点での全確定ノードリスト (前回までの確定ノード + 今回確定したノード)
                      ARRAY_CONCAT(
                        prev.settled_nodes,
                        (SELECT ARRAY(
                          SELECT AS STRUCT nbs_check.nbs_node AS node, nbs_check.nbs_cost AS cost, nbs_check.nbs_path AS path FROM (
                            SELECT nbs_calc.node AS nbs_node, nbs_calc.cost AS nbs_cost, nbs_calc.path AS nbs_path
                            FROM UNNEST(prev.unsettled_nodes) AS nbs_calc ORDER BY nbs_calc.cost ASC, ARRAY_TO_STRING(nbs_calc.path, '->') ASC LIMIT 1
                          ) AS nbs_check
                        ))
                      )
                    ) s_node
                    WHERE s_node.node = e.target_node
                  )
                  AND e.target_node IS NOT NULL -- エッジの終点がNULLでないことを保証
            ) AS all_cand
            WHERE all_cand.node IS NOT NULL -- UNION ALLの結果、nodeがNULLになるケースを除外 (主に空のnbs_infoからのexpanded_from_settled対策)
        ) AS ranked
        WHERE ranked.rn = 1 -- 各ノードについて最小コストのパスのみを新しい未確定リストに残す
      ) AS unsettled_nodes

    FROM dijkstra_iterations prev -- 再帰的に自身を参照
    CROSS JOIN node_count -- 総ノード数を参照するため
    -- 再帰の終了条件
    WHERE
      ARRAY_LENGTH(prev.unsettled_nodes) > 0     -- 未確定ノードが存在する間
      AND prev.iteration < node_count.cnt        -- 最大反復回数に達していない (全ノード処理する前に終わるはず)
      AND NOT EXISTS (SELECT 1 FROM UNNEST(prev.settled_nodes) s WHERE s.node = 'F') -- 目的ノード'F'が確定したら終了
  )

-- 最終結果の選択:
-- 目的ノード'F'が確定済みノードのリストに含まれていれば、その最短コストと経路を取得します。
SELECT arr.path, arr.cost
FROM dijkstra_iterations, UNNEST(settled_nodes) AS arr
WHERE arr.node = 'F' -- 目的ノード
  AND iteration = ( -- 目的ノードが最初に確定したイテレーションの結果を選択
    SELECT MIN(it.iteration)
    FROM dijkstra_iterations it, UNNEST(it.settled_nodes) AS s_arr
    WHERE s_arr.node = 'F'
  )
ORDER BY arr.cost ASC -- 通常は1レコードのはずですが、念のためコストでソート
LIMIT 1; -- 最短経路が複数ある場合でも1つだけ選択
