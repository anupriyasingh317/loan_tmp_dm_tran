create or replace package body loan_service_pkg is
procedure process_loan_data is 
Begin
INSERT INTO tmp_dm_tran
        (bch_id,
         tran_id,
         run_ptit_id,
         fnm_ln_id,
         fncl_ast_id,
         ln_dlqy_mdfc_dt,
         ln_bef_mdfc_amrt_typ_cd,
         ln_aft_mdfc_amrt_typ_cd,
         ln_int_cplzd_amt,
         ln_upb_cplzd_amt,
         ln_mtry_dt,
         ln_upb_frgv_amt,
         ln_int_frgv_amt,
         ln_uapd_fnds_apld_to_int_amt,
         ln_upb_bef_mdfc_amt,
         ln_srvg_fee_rt,
         ln_lndr_ptr,
         ln_excs_yld_rt,
         acp_tran_id,
         tran_smss_src_cd,
         fnm_sys_sstr_typ_cd,
         acp_tran_st_cd,
         acp_tran_st_chg_usr_id,
         ln_multi_step_ind,
         ln_disaster_ind,
         ln_disaster_nme,
         ln_bef_lpi_dt,
         darts_id,
         ln_upb_cplzd_othr_amt,
         ln_srvg_fee_amt,
         selr_srvr_no,
         ln_new_upb_amt,
         tran_typ_cd,
         ln_bwkly_to_mthy_ind,
         ln_new_lpi_dt,
         ln_rimb_prin_amt,
         ln_rimb_lpt_amt,
         ln_dlqy_mdfc_cncln_ind,
         ln_wrkot_new_typ_cd,
         ln_prin_frbrn_new_amt,
         ---loan table attributes
         ln_stat_cd,
         ln_core_cvrn_dt,
         ln_fncl_acvy_archv_dt,
         prop_pur_prc_amt,
         ln_fst_inlm_due_dt,
         ln_fnm_acqrd_pct,
         ln_aqsn_actl_upb_amt,
         ln_int_only_end_dt,
         ln_rmtt_typ_cd,
         ln_mdfc_alwd_ind,
         acp_chg_typ_dt,
         ln_int_accrl_frqy_mthd_cd,
         ln_orig_upb_amt,
         srvr_lndr_ln_id,
         ln_lpi_at_sir_setup_dt,
         ln_old_srvg_fee_rt,
         ln_wrkot_old_typ_cd,
         ln_prin_frbrn_old_amt,
         -- Loan Common Reference Attributes
         ltst_ln_apld_fncl_st_id,
         ltst_ln_apld_pdc_acvy_st_id,
         srvr_prty_id,
         ltst_lpc_tran_seq_no,
         -- Loan feature attributes
         ln_sle_typ_cd,
         ln_fcl_ls_rsk_cd,
         ln_max_term,
         prcs_dt_clctn_id,
         pmt_cyl_id,
         orig_prcd_dt_clctn_id,
         ltst_ln_feat_id,
         ltst_ln_feat_expt_dt,
         ltst_ln_feat_eff_dt,
         ln_rptg_mthd_cd,
         ln_fnm_owns_pct,
         ln_prdc_lbl_typ_cd,
         --loan Applied Financial State Attributes
         ln_days_dlq_cnt,
         ln_upb_amt,
         ln_lpi_dt,
         ln_apld_fncl_st_seq_no,
         old_ln_rimb_prin_amt,
         old_ln_rimb_lpt_amt,
         ln_apld_schd_upb_amt,
         ln_rmng_term,
         ln_old_fctrd_upb_amt,
         ln_grs_upb_amt,
         --Process DT Collection
         rptg_frqy_cd,
         --Sequence Values
         acp_actg_evnt_id,
         ln_apld_fncl_st_id,
         lpc_tran_id,
         orig_prcs_dt_clctn_id,
         ln_new_pmt_rt_st_id,
         ln_odd_due_cnvr_ind,
         pr_pmt_cyl_id,
         brwr_mdfd_mthy_exp_amt,
         brwr_mdfd_mthy_incm_amt,
         ln_tran_typ_cd,
         expd_lpi_dt,
         ln_lpt_int_advng_stop_typ_cd,
         ln_pdp_ind,
         ln_pmt_rt_prjtn_ind,
         ln_aqsn_dt,
         ln_old_cum_sfee_frbrn_amt,
         ln_old_cum_gfee_frbrn_amt
         )
        SELECT ldms.bch_id,
               ldms.tran_id,
               ldms.run_ptit_id,
               ldms.fnm_ln_id,
               ldms.fncl_ast_id,
               ldms.ln_dlqy_mdfc_dt,
               ldms.ln_bef_mdfc_amrt_typ_cd,
               ldms.ln_aft_mdfc_amrt_typ_cd,
               nvl(ldms.ln_int_cplzd_amt, 0),
               nvl(ldms.ln_upb_cplzd_amt, 0),
               ldms.ln_mtry_dt,
               nvl(ldms.ln_upb_frgv_amt, 0),
               nvl(ldms.ln_int_frgv_amt, 0),
               nvl(ldms.ln_uapd_fnds_apld_to_int_amt, 0),
               nvl(ldms.ln_upb_bef_mdfc_amt, 0),
               nvl(ldms.ln_srvg_fee_rt, 0),
               nvl(ldms.ln_lndr_ptr, 0),
               nvl(ldms.ln_excs_yld_rt, 0),
               nvl(ldms.acp_tran_id, acp_tran_id_seq.NEXTVAL) acp_tran_id,
               nvl(ldms.tran_smss_src_cd, pkg_ztran_smss_src.gl_srvr_bch) tran_smss_src_cd,
               pkg_zfnm_sys_sstr_typ.gl_acp fnm_sys_sstr_typ_cd,
               decode(nvl(ldms.acp_tran_st_cd, pkg_zacp_tran_st.gl_cpld),
                      pkg_zacp_tran_st.gl_smtd,
                      pkg_zacp_tran_st.gl_cpld,
                      nvl(ldms.acp_tran_st_cd, pkg_zacp_tran_st.gl_cpld)) acp_tran_st_cd, --If it null,Default to Completed
               ldms.acp_tran_st_chg_usr_id,
               upper(ldms.ln_mult_step_ind),
               upper(decode(ldms.tran_typ_cd,
                            pkg_ztran_typ.gl_dm,
                            nvl(ldms.ln_dsastr_ind, pkg_core.gl_false),
                            pkg_ztran_typ.gl_bnkr_cram_dw,
                            pkg_core.gl_true,
                            pkg_core.gl_true)) ln_disaster_ind,
               ldms.ln_dsastr_nme,
               nvl(ldms.ln_bef_cplzd_lpi_dt, lafs.ln_lpi_dt) ln_bef_lpi_dt,
               ldms.ln_darts_id,
               nvl(ldms.ln_upb_cplzd_amt, 0)- nvl(ldms.ln_int_cplzd_amt, 0) ln_upb_cplzd_othr_amt,
               nvl(ldms.ln_srvg_fee_amt, 0),
               lcr.srvr_prty_id,--selr_srvr_no (SFLSTSVC-73996)
               nvl(ldms.ln_upb_amt, lafs.ln_upb_amt + nvl(l.ln_prin_frbrn_amt,0) + (nvl(ldms.ln_upb_cplzd_amt, 0.0) -
                   nvl(ldms.ln_upb_frgv_amt, 0.0)) - nvl(ldms.ln_prin_frbrn_amt,0)) ln_new_upb_amt,
               ldms.tran_typ_cd,
               (CASE
                 -- SSSECSVC11-1309 Bi_Weekly loans should remain as Bi-weekly as part of the Payment Deferral No Reclass Program.
                 WHEN ldms.ln_pdp_ind = pkg_core.gl_true THEN
                   pkg_core.gl_false
                 WHEN pdc.rptg_frqy_cd = pkg_zfrqy.gl_bwkly THEN
                   NVL(ldms.ln_bwkly_to_mthy_ind, pkg_core.gl_true)
                 ELSE
                   pkg_core.gl_false
                END
               ) ln_bwkly_to_mthy_ind,
               ldms.ln_lpi_dt,
               NVL(ldms.ln_rimb_prin_amt,0),
               NVL(ldms.ln_rimb_lpt_amt,0),
               NVL(ldms.ln_dlqy_mdfc_cncln_ind, pkg_core.gl_false),
               ldms.ln_wrkot_typ_cd,
               ldms.ln_prin_frbrn_amt,
               ---loan table attributes
               l.ln_stat_cd,
               l.ln_core_cvrn_dt,
               l.ln_fncl_acvy_archv_dt,
               l.prop_pur_prc_amt,
               l.ln_fst_inlm_due_dt,
               nvl(l.ln_fnm_acqrd_pct, 0.0),
               l.ln_aqsn_actl_upb_amt,
               l.ln_int_only_end_dt,
               l.ln_rmtt_typ_cd,
               l.ln_mdfc_alwd_ind,
               l.acp_chg_typ_dt,
               l.ln_int_accrl_frqy_mthd_cd,
               l.ln_orig_upb_amt,
               l.srvr_lndr_ln_id,
               l.ln_lpi_at_sir_setup_dt,
               l.ln_min_srvg_fee_rt,
               l.ln_wrkot_typ_cd,
               l.ln_prin_frbrn_amt,
               -- Loan Common Reference Attributes
               lcr.ltst_ln_apld_fncl_st_id,
               lcr.ltst_ln_apld_pdc_acvy_st_id,
               lcr.srvr_prty_id,
               lcr.ltst_lpc_tran_seq_no,
               -- Loan feature attributes
               lf.ln_sle_typ_cd,
               lf.ln_fcl_ls_rsk_cd,
               l.ln_max_term,
               CASE
                 -- SSSECSVC11-1309
                 WHEN ldms.ln_pdp_ind = pkg_core.gl_true
                 THEN l.prcs_dt_clctn_id
                 WHEN pdc.rptg_frqy_cd = pkg_zfrqy.gl_bwkly
                  AND NVL(ldms.ln_bwkly_to_mthy_ind, pkg_core.gl_true) = pkg_core.gl_true
                 THEN l_mthy_aa_sa_smry_pdcs_id -- 26
                 ELSE l.prcs_dt_clctn_id
               END prcs_dt_clctn_id,
               CASE
                 -- SSSECSVC11-1309
                 WHEN ldms.ln_pdp_ind = pkg_core.gl_true
                 THEN l.pmt_cyl_id
                 WHEN pdc.rptg_frqy_cd = pkg_zfrqy.gl_bwkly
                  AND NVL(ldms.ln_bwkly_to_mthy_ind, pkg_core.gl_true) = pkg_core.gl_true
                 THEN l_mthy_due_1st_pmt_cyl_id
                 WHEN pdc.rptg_frqy_cd = pkg_zfrqy.gl_mthy
                  AND l.pmt_cyl_id BETWEEN 2 AND 31
                  AND EXTRACT(DAY FROM ldms.ln_dlqy_mdfc_dt) <> l.pmt_cyl_id
                 THEN l_mthy_due_1st_pmt_cyl_id
                 ELSE l.pmt_cyl_id
               END pmt_cyl_id,
               l.prcs_dt_clctn_id orig_prcd_dt_clctn_id,
               lf.ln_feat_id,
               lf.ln_feat_expt_dt,
               lf.ln_feat_eff_dt,
               l.ln_rptg_mthd_cd,
               lf.ln_fnm_owns_pct,
               lf.ln_prdc_lbl_typ_cd,
               --loan Applied Financial State Attributes
               lafs.ln_days_dlq_cnt,
               lafs.ln_upb_amt,
               lafs.ln_lpi_dt,
               lafs.ln_apld_fncl_st_seq_no,
               lafs.ln_tot_prin_advd_amt,
               lafs.ln_tot_lpt_int_advd_amt,
               lafs.ln_schd_upb_amt,
               lafs.ln_rmng_term,
               lafs.ln_fctrd_upb_amt,
               (CASE
                 WHEN l.ln_rmtt_typ_cd = pkg_zln_rmtt_typ.GL_S_S
                 THEN nvl(lafs.ln_fctrd_schd_upb_amt, 0) + (nvl(l.ln_prin_frbrn_amt, 0) * nvl(l.ln_fnm_acqrd_pct, 0))
                 ELSE nvl(lafs.ln_fctrd_upb_amt, 0) + (nvl(l.ln_prin_frbrn_amt, 0) * nvl(l.ln_fnm_acqrd_pct, 0))
               END) ln_grs_upb_amt,
               --Process DT Collection
               pdc.rptg_frqy_cd,
               --Sequence.NextVals (ACP_ACNTG,LAFS,LPC_TRAN_ID)
               acp_actg_evnt_id_seq.NEXTVAL   acp_actg_evnt_id,
               ln_apld_fncl_st_id_seq.NEXTVAL new_ln_apld_fncl_st_id,
               lpc_tran_id_seq.NEXTVAL        new_lpc_tran_id,
               l.prcs_dt_clctn_id,
               CASE
                 WHEN ldms.ln_pdp_ind = pkg_core.gl_true
                 THEN NULL
                 ELSE ln_pmt_rt_st_id_seq.NEXTVAL
               END new_pmt_rt_st_id,
               -- Convert Odd Due to Due On 1st
               CASE
                 -- SSSECSVC11-1309 Odd Due loans should remain as Odd Due as part of the Payment Deferral No Reclass Program.
                 WHEN ldms.ln_pdp_ind = pkg_core.gl_true
                 THEN pkg_core.gl_false
                 WHEN pdc.rptg_frqy_cd = pkg_zfrqy.gl_mthy
                  AND l.pmt_cyl_id BETWEEN 2 AND 31
                  AND EXTRACT(DAY FROM ldms.ln_dlqy_mdfc_dt) <> l.pmt_cyl_id
                 THEN pkg_core.gl_true
                 ELSE pkg_core.gl_false
               END ln_odd_due_cnvr_ind ,
               l.pmt_cyl_id pr_pmt_cyl_id,
               ldms.brwr_mdfd_mthy_exp_amt,
               ldms.brwr_mdfd_mthy_incm_amt,
               l.tran_typ_cd,
               NULL expd_lpi_dt,
               lafs.ln_lpt_int_advng_stop_typ_cd,
               -- SSSECSVC11-1497 indicator added to determine if workout type code is part of PDP.
               -- Will be used throughout delmod for areas impacted by PDP.
               ldms.ln_pdp_ind,
         -- SSSECSVC11-1553 indicator added to determine if payment and rate projection should
         -- occur throughout the delmod process. For PDP loans, no projection should occur.
               CASE
                 WHEN ldms.ln_pdp_ind = pkg_core.gl_true
                 THEN pkg_core.gl_false
                 ELSE pkg_core.gl_true
               END ln_pmt_rt_prjtn_ind,
               l.ln_aqsn_dt,
         -- SSSECSVC11-2416 Calculate cumulative GFee and SFee reimbursement amount when PD is processed
               nvl(l.ln_cum_sfee_frbrn_amt,0) ln_old_cum_sfee_frbrn_amt,
               nvl(l.ln_cum_gfee_frbrn_amt,0) ln_old_cum_gfee_frbrn_amt
          FROM ln_dlqy_mdfc_stgg ldms
          JOIN ln_cmn_ref lcr ON (ldms.fncl_ast_id = lcr.fncl_ast_id)
          JOIN ln_feat lf ON (lcr.ltst_ln_feat_id = lf.ln_feat_id)
          JOIN loan l ON (l.fncl_ast_id = ldms.fncl_ast_id)
          JOIN ln_apld_fncl_st lafs ON (lcr.ltst_ln_apld_fncl_st_id =
                                       lafs.ln_apld_fncl_st_id AND
                                       lcr.ltst_ln_apld_pdc_acvy_st_id =
                                       lafs.prcs_dt_clctn_st_acvy_id)
          JOIN prcs_dt_clctn pdc ON (pdc.prcs_dt_clctn_id = l.prcs_dt_clctn_id)
         WHERE ldms.run_ptit_id = i_run_part_id
           AND ldms.tran_id = i_tx_id
           AND ldms.bch_id = i_batch_id
           AND nvl(ldms.acp_tran_st_cd, pkg_zacp_tran_st.gl_cpld) <> pkg_zacp_tran_st.gl_cncld;
		   End;
End loan_service_pkg;
