import pandas as pd

def transform_cluster_counts(df):
    # userId, cluster_num(클러스터 결과)만 선택
    df = df[["userId", "cluster_num"]]

    # userId별 cluster_num 개수 집계 (pivot 형태)
    cluster_df = df.groupby(["userId", "cluster_num"]).size().unstack(fill_value=0)

    # cluster_1 ~ cluster_5 컬럼 이름 붙이기
    cluster_df.columns = [f"cluster_{int(col)}" for col in cluster_df.columns]

    # cluster_1 ~ cluster_5 모두 있는지 확인하고, 없는 것은 0으로 추가
    for i in range(1, 6):
        col = f"cluster_{i}"
        if col not in cluster_df.columns:
            cluster_df[col] = 0

    # 컬럼 순서 정렬
    cluster_df = cluster_df[[f"cluster_{i}" for i in range(1, 6)]]
    cluster_df.reset_index(inplace=True)

    return cluster_df

