-- 例題5: 無理やりダイクストラ

WITH RECURSIVE
  -- グラフのエッジとコストを定義
  edges AS (
    SELECT 'A' AS source_node, 'B' AS target_node, 3 AS cost UNION ALL
    SELECT 'A', 'C', 4 UNION ALL
    SELECT 'B', 'D', 2 UNION ALL
    SELECT 'B', 'E', 6 UNION ALL
    SELECT 'C', 'E', 1 UNION ALL
    SELECT 'D', 'E', 3 UNION ALL
    SELECT 'D', 'F', 7 UNION ALL
    SELECT 'E', 'F', 6
  ),

  -- ダイクストラ法を模倣する再帰処理
  -- iteration: 反復回数
  -- current_node: 現在のノード
  -- total_cost: スタートノードからの総コスト
  -- path: スタートノードからの経路
  -- visited: 訪問済みノードの配列（ループ防止と未訪問ノードの選択に使用）
  PathsRecursive AS (
    -- 初期状態：スタートノード 'A'
    SELECT
      0 AS iteration,
      'A' AS current_node,
      0 AS total_cost,
      ['A'] AS path,
      ['A'] AS visited

    UNION ALL

    -- 反復ステップ
    SELECT
      prev.iteration + 1 AS iteration,
      e.target_node AS current_node,
      prev.total_cost + e.cost AS total_cost,
      ARRAY_CONCAT(prev.path, [e.target_node]) AS path,
      ARRAY_CONCAT(prev.visited, [e.target_node]) AS visited
    FROM
      PathsRecursive AS prev
      JOIN edges AS e ON prev.current_node = e.source_node or prev.current_node = e.target_node  -- 無向グラフなので双方向に進む
    WHERE
      -- まだ訪問していないノードにのみ進む
      NOT e.target_node IN UNNEST(prev.visited)
      -- 再帰の深さ制限（必要に応じて調整）
      AND prev.iteration < (SELECT COUNT(DISTINCT node) FROM (SELECT source_node AS node FROM edges UNION ALL SELECT target_node AS node FROM edges))
  ),

  -- 各ノードへの最短コストを計算
  min_costs AS (
    SELECT
      current_node,
      MIN(total_cost) AS min_total_cost
    FROM PathsRecursive
    GROUP BY current_node
  )

-- スタートノード 'A' から ゴールノード 'F' への最短経路とコストを取得
SELECT
  pr.path,
  pr.total_cost
FROM
  PathsRecursive AS pr
  JOIN min_costs AS mc
    ON pr.current_node = mc.current_node AND pr.total_cost = mc.min_total_cost
WHERE
  pr.current_node = 'F' -- ゴールノード
ORDER BY
  pr.total_cost ASC
LIMIT 1;
