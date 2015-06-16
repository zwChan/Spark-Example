INSERT overwrite TABLE TESTDW.rpt_shop_session_detl PARTITION (ds = '2016-06-01') 

SELECT
    x.ds,
    x.session_id,
    x.shop_id,
    x.hp_pv,
    x.hp_2,
    x.hp_dtl,
    x.dtl_pv,
    x.dtl_2,
    x.ud_pv,
    x.ud_2,
    x.ud_dtl,
    x.cls_pv,
    x.cls_2,
    x.cls_dtl,
    x.cart_pv,
    x.order_num,
    x.vaild_order_num,
    GetDateTime () AS upt_time,
    sc.pltfm_id,
    sc.refer_page_id,
    sc.chanl_id
FROM
      (
        SELECT
                ds,
                session_id,
                shop_id,
                count(
                        DISTINCT CASE
                        WHEN page_type_id = 16 THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS hp_pv,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) = 16
                        AND CAST(page_type_id AS INT)  IN (
                                16,
                                221,
                                222,
                                223,
                                224,
                                225,
                                15,
                                123,
                                295
                        ) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS hp_2,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) = 16
                        AND CAST(page_type_id AS INT)  IN (15, 123, 295) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS hp_dtl,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) IN (16, 221, 222, 223, 224, 225)
                        AND CAST(page_type_id AS INT)  IN (123, 15, 295) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS dtl_pv,
                0 AS dtl_2,
                count(
                        DISTINCT CASE
                        WHEN CAST(page_type_id AS INT) IN (223, 224) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS ud_pv,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) IN (223, 224)
                        AND CAST(page_type_id AS INT) IN (
                                16,
                                221,
                                222,
                                223,
                                224,
                                225,
                                15,
                                123,
                                295
                        ) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS ud_2,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) IN (223, 224)
                        AND CAST(page_type_id AS INT) IN (15, 123, 295) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS ud_dtl,
                count(
                        DISTINCT CASE
                        WHEN CAST(page_type_id AS INT) IN (221, 222) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) AS cls_pv,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) IN (221, 222)
                        AND CAST(page_type_id AS INT) IN (
                                16,
                                221,
                                222,
                                223,
                                224,
                                225,
                                15,
                                123,
                                295
                        ) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) cls_2,
                count(
                        DISTINCT CASE
                        WHEN CAST(ref_page_type_id AS INT) IN (221, 222)
                        AND CAST(page_type_id AS INT) IN (15, 123, 295) THEN
                                CASE
                        WHEN tracker_id = '' THEN
                                NULL
                        ELSE
                                tracker_id
                        END
                        ELSE
                                NULL
                        END
                ) cls_dtl,
                count(
                        DISTINCT CASE
                        WHEN CAST(nav_page_type_id AS INT) IN (16, 221, 222, 223, 224, 225)
                        AND length(
                                CASE
                                WHEN cart_page_track_id = '' THEN
                                        NULL
                                ELSE
                                        cart_page_track_id
                                END
                        ) > 0 THEN
                                CASE
                        WHEN cart_page_track_id = '' THEN
                                NULL
                        ELSE
                                cart_page_track_id
                        END
                        ELSE
                                NULL
                        END
                ) cart_pv,
                count(
                        DISTINCT CASE
                        WHEN CAST(nav_page_type_id AS INT) IN (16, 221, 222, 223, 224, 225) THEN
                                CASE
                        WHEN a.ordr_code = '' THEN
                                NULL
                        ELSE
                                a.ordr_code
                        END
                        ELSE
                                NULL
                        END
                ) order_num,
                count(
                        DISTINCT CASE
                        WHEN CAST(nav_page_type_id AS INT) IN (16, 221, 222, 223, 224, 225) THEN
                                b.parnt_ordr_code
                        ELSE
                                NULL
                        END
                ) vaild_order_num
        FROM
                DW.FCT_TRAFFIC_SHOP_DETL a
        LEFT OUTER JOIN (
                SELECT DISTINCT
                        parnt_ordr_code
                FROM
                        DW.dim_ordr
                WHERE
                        ds = '2015-06-01'
                AND ordr_activ_flg = 1
        ) b ON a.ordr_code = b.parnt_ordr_code
        WHERE
                ds = '2015-06-01'
        AND (
                CAST(nav_page_type_id AS INT) IN (16, 221, 222, 223, 224, 225)
                OR CAST(ref_page_type_id AS INT) IN (16, 221, 222, 223, 224, 225)
                OR CAST(page_type_id AS INT) IN (16, 221, 222, 223, 224, 225)
        )
        AND length(shop_id) > 0
        GROUP BY
            ds,
            session_id,
            shop_id
        UNION ALL
            SELECT
                ds,
                session_id,
                shop_id,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(page_type_id AS INT) = 314 THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) AS hp_pv,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(ref_page_type_id AS INT) = 314
                    AND CAST(page_type_id AS INT)  IN (
                        314,
                        315,
                        316,
                        104,
                        107,
                        319,
                        272,
                        354,
                        355
                    ) THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) AS hp_2,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(ref_page_type_id AS INT) = 314
                    AND CAST(page_type_id AS INT)  IN (104, 107, 319, 272) THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) AS hp_dtl,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(ref_page_type_id AS INT) IN (314, 315, 316, 354, 355)
                    AND CAST(page_type_id AS INT)  IN (104, 107, 319, 272) THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) dtl_pv,
                0 AS dtl_2,
                0 AS ud_pv,
                0 AS ud_2,
                0 AS ud_dtl,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(page_type_id AS INT) IN (315, 316, 354, 355) THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) AS cls_pv,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(ref_page_type_id AS INT) IN (315, 316, 354, 355)
                    AND CAST(page_type_id AS INT)  IN (
                        314,
                        315,
                        316,
                        104,
                        107,
                        319,
                        272,
                        354,
                        355
                    ) THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) AS cls_2,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(ref_page_type_id AS INT) IN (315, 316, 354, 355)
                    AND CAST(page_type_id AS INT)  IN (104, 107, 319, 272, 354, 355) THEN
                        CASE
                    WHEN tracker_id = '' THEN
                        NULL
                    ELSE
                        tracker_id
                    END
                    ELSE
                        NULL
                    END
                ) AS cls_dtl,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(nav_page_type_id AS INT) IN (314, 315, 316, 354, 355)
                    AND LENGTH (
                        CASE
                        WHEN cart_page_track_id = '' THEN
                            NULL
                        ELSE
                            cart_page_track_id
                        END
                    ) > 0 THEN
                        CASE
                    WHEN cart_page_track_id = '' THEN
                        NULL
                    ELSE
                        cart_page_track_id
                    END
                    ELSE
                        NULL
                    END
                ) AS cart_pv,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(nav_page_type_id AS INT) IN (314, 315, 316, 354, 355) THEN
                        CASE
                    WHEN A .ordr_code = '' THEN
                        NULL
                    ELSE
                        A .ordr_code
                    END
                    ELSE
                        NULL
                    END
                ) AS order_num,
                COUNT (
                    DISTINCT CASE
                    WHEN CAST(nav_page_type_id AS INT) IN (314, 315, 316, 354, 355) THEN
                        b.parnt_ordr_code
                    ELSE
                        NULL
                    END
                ) AS vaild_order_num
            FROM
                DW.FCT_TRAFFIC_SHOP_DETL A
            LEFT OUTER JOIN (
                SELECT DISTINCT
                    parnt_ordr_code
                FROM
                    DW.dim_ordr
                WHERE
                    ds = '2016-06-01'
                AND ordr_activ_flg = 1
            ) b ON A .ordr_code = b.parnt_ordr_code
            WHERE
                ds = '2016-06-01'
            AND (
                CAST(nav_page_type_id AS INT) IN (314, 315, 316, 354, 355)
                OR CAST(ref_page_type_id AS INT) IN (314, 315, 316, 354, 355)
                OR CAST(page_type_id AS INT) IN (314, 315, 316, 354, 355)
            )
            AND LENGTH (shop_id) > 0
            GROUP BY
                ds,
                session_id,
                shop_id
    ) x
JOIN TESTDW.tmp_shop_session_channel sc ON sc.session_id = x.session_id
AND sc.shop_id = x.shop_id;
