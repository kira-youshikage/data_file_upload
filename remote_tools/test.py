CREATE TABLE "dm_dms"."ML_SEM_XS_DATA" (
  "slider ocr no" VARCHAR(255),
  "wafer" VARCHAR(255),
  "block" VARCHAR(255),
  "row" VARCHAR(255),
  "slider no" VARCHAR(255),
  "sem_type" VARCHAR(255),
  "photo_type" VARCHAR(255),
  "sem_mode" VARCHAR(255),
  "matrix_name" VARCHAR(255),
  "accelerating_voltage" VARCHAR(255),
  "magnification" VARCHAR(255),
  "sem_job_id" VARCHAR(255),
  "ori_file_path" VARCHAR(255),
  "pred_file_path" VARCHAR(255),
  "completion_time" DATETIME,
  "creation_time" DATETIME,
  "image_url" VARCHAR(255),
  "job_id" VARCHAR(255),
  "job_status" VARCHAR(255),
  "job_type" VARCHAR(255),
  "status" VARCHAR(255),
  "pred_pp3_pb" DOUBLE,
  "pred_pp3x" DOUBLE,
  "pred_th" DOUBLE,
  "pred_th2" DOUBLE,
  "pred_tyd" DOUBLE,
  "pred_tyt" DOUBLE,
  "pred_ws1t" DOUBLE,
  "pred_l_sh_max" DOUBLE,
  "pred_l_sh_min" DOUBLE,
  "pred_l_st1" DOUBLE,
  "pred_l_st2" DOUBLE,
  "pred_lsh_min_2" DOUBLE,
  "pred_pt2_max" DOUBLE,
  "pred_tby_d2" DOUBLE,
  "pred_tby_t" DOUBLE,
  "pred_dws_gs" DOUBLE,
  "pred_le_ta1" DOUBLE,
  "pred_le_ta2" DOUBLE,
  "pred_let_d" DOUBLE,
  "pred_let_t" DOUBLE,
  "pred_lsg_x" DOUBLE,
  "pred_lst_d" DOUBLE,
  "pred_lst_t" DOUBLE,
  "pred_mlg" DOUBLE,
  "pred_pt1" DOUBLE,
  "pred_pt2m" DOUBLE,
  "pred_pt2m_2" DOUBLE,
  "pred_tby_d1" DOUBLE,
  "pred_th1" DOUBLE,
  "pred_twg_deg" DOUBLE,
  "pred_twg_dm" DOUBLE,
  "pred_twg_tm" DOUBLE,
  "pred_et_ht_100" DOUBLE,
  "pred_eth_d" DOUBLE,
  "pred_eth_t" DOUBLE,
  "pred_gmr_h" DOUBLE,
  "pred_gmr_h_2" DOUBLE,
  "pred_tt_hd" DOUBLE,
  "pred_wgx_1" DOUBLE,
  "pred_wgx_2" DOUBLE,
 PRIMARY KEY ("slider ocr no" ,"accelerating_voltage" ,"magnification" )
);