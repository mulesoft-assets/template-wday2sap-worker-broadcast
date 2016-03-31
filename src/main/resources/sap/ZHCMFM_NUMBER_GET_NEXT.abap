*----------------------------------------------------------------------*
* Project        : MuleSoft                                            *
* Requirement N°.: JIRA->SAPINFRA-8 / Redmine->#3479                   *
* Function Group : ZHCMFG_EMPLOYEE                                     *
* Function Module: ZHCMFM_NUMBER_GET_NEXT                              *
* Created by     : Martín E. Isnardi                                   *
* Creation date  : 11.03.2016                                          *
* Description    : Get list of Materials                               *
* Transport      : IDEK900068                                          *
*----------------------------------------------------------------------*
* Modified by    :                                                     *
* Requirement N° :                                                     *
* Change ID      :                                                     *
* Date           : dd.mm.aaaa                                          *
* Description    :                                                     *
* Transport      :                                                     *
*----------------------------------------------------------------------*
FUNCTION zhcmfm_number_get_next.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IV_NR_RANGE_NR) TYPE  INRI-NRRANGENR OPTIONAL
*"     VALUE(IV_OBJECT) TYPE  INRI-OBJECT OPTIONAL
*"     VALUE(IV_QUANTITY) TYPE  INRI-QUANTITY DEFAULT '1'
*"     VALUE(IV_SUBOBJECT) TYPE  NRSOBJ DEFAULT SPACE
*"     VALUE(IV_TOYEAR) TYPE  INRI-TOYEAR DEFAULT '0000'
*"     VALUE(IV_IGNORE_BUFFER) TYPE  CHAR1 DEFAULT SPACE
*"  EXPORTING
*"     VALUE(EV_NUMBER) TYPE  PERSNO
*"     VALUE(EV_RETURNCODE) TYPE  INRI-RETURNCODE
*"----------------------------------------------------------------------

* MuleSoft: Get next employee no in n.range
* call standard FM in case of internal number range
* Global data declarations

* Function module documentation

  CALL FUNCTION 'NUMBER_GET_NEXT'
    EXPORTING
      nr_range_nr             = iv_nr_range_nr
      object                  = iv_object
      quantity                = iv_quantity "'1'
      subobject               = iv_subobject "' '
      toyear                  = iv_toyear "'0000'
      ignore_buffer           = iv_ignore_buffer "' '
    IMPORTING
      number                  = ev_number
      quantity                = iv_quantity
      returncode              = ev_returncode
    EXCEPTIONS
      interval_not_found      = 1
      number_range_not_intern = 2
      object_not_found        = 3
      quantity_is_0           = 4
      quantity_is_not_1       = 5
      interval_overflow       = 6
      buffer_overflow         = 7
      OTHERS                  = 8.

  ev_returncode = sy-subrc.

  IF ev_returncode EQ 2.
* use custom login in case of external number range
    SELECT MAX( pernr )
          FROM pa0003
          INTO ev_number.
    IF sy-subrc EQ 0.
      ev_number = ev_number + 1.
    ENDIF.
  ENDIF.



  "*New data declarations for advanced local file buffering
  "data: h_length type i,
  " h_offset type i.
  "* .. declarations for WP logical ID number determination ..
  "data: wp(2) type c,
  " pid(8) type c,
  "* instlen type I,
  " wpindex type INT4,
  " wp_index(8) type c,
  " opcode type x.
  "*save function group global defined variable for instance name.
  "data: z_db_group_key like %_db_group_key.
  "data: zz_db_group_key(23) type c.
  "data: shadow_key(43) type c.
  " DATA: H_EXIT.
  " DATA: L_NRIV LIKE NRIV.

  "*********BEGIN KAYAK***************************************************
  " DATA:
  " lr_badi TYPE REF TO badi_number_get_next,
  " lv_exit TYPE char1,
  " lv_exception TYPE rsfbpara-parameter.

  " GET BADI lr_badi.
  " CALL BADI lr_badi->number_get
  " EXPORTING
  " iv_nr_range_nr = nr_range_nr
  " iv_object = object
  " iv_quantity = quantity
  " iv_subobject = subobject
  " iv_toyear = toyear
  " iv_ignore_buffer = ignore_buffer
  " IMPORTING
  " ev_number = number
  " ev_quantity = quantity
  " ev_returncode = returncode
  " ev_exit = lv_exit
  " ev_exception = lv_exception.
  " CASE lv_exception.
  " WHEN space.
  " IF lv_exit = abap_true.
  " EXIT.
  " ENDIF.
  " WHEN 'INTERVAL_NOT_FOUND'.
  " RAISE interval_not_found.
  " WHEN 'NUMBER_RANGE_NOT_INTERN'.
  " RAISE number_range_not_intern.
  " WHEN 'OBJECT_NOT_FOUND'.
  " RAISE object_not_found.
  " WHEN 'QUANTITY_IS_0'.
  " RAISE quantity_is_0.
  " WHEN 'QUANTITY_IS_NOT_1'.
  " RAISE quantity_is_not_1.
  " WHEN 'INTERVAL_OVERFLOW'.
  " RAISE interval_overflow.
  " WHEN 'BUFFER_OVERFLOW'.
  " RAISE buffer_overflow.
  " ENDCASE.
  "*********END KAYAK*****************************************************

  "* Missing initialisation for function group global defined variable
  "* buffer_active = 'X'.
  " z_db_group_key = %_db_group_key.
  "* Nummernkreisobjekt nur lesen, wenn OBJECT vom zuletzt gelesenen
  "* Objekt abweicht.
  " IF ACT_OBJECT = SPACE OR ACT_OBJECT NE OBJECT.
  " PERFORM READ_TNRO USING OBJECT.
  " ENDIF.

  " IF QUANTITY = 0.
  "* raise quantity_is_0.
  " MESSAGE E029 RAISING QUANTITY_IS_0.
  " ENDIF.

  " IF TNRO-BUFFER = LOCAL AND QUANTITY <> 1.
  "* raise quantity_is_not_1.
  " MESSAGE E030 RAISING QUANTITY_IS_NOT_1.
  " ENDIF.
  " if tnro-buffer = process and quantity <> 1.
  "* raise quantity_is_not_1.
  " message e030 raising quantity_is_not_1.
  " endif.
  " if tnro-buffer = shadow and quantity <> 1.
  "* raise quantity_is_not_1.
  " message e030 raising quantity_is_not_1.
  " endif.

  "* now all the errors and resetting are handled by the kernel
  " if tnro-buffer = no or ignore_buffer = 'X'.
  "* no buffering or buffering should be ignored
  " PERFORM READ_NRIV USING OBJECT SUBOBJECT NR_RANGE_NR TOYEAR
  " QUANTITY RETURNCODE NUMBER.
  " exit.
  " endif.

  " IF TNRO-BUFFER = YES.
  "* Die Intervalle sind gepuffert und sollen aus dem Puffer über
  "* den Nummernkreisserver besorgt werden.
  "* Schnittstelle der C-Routine versorgen
  " PERFORM LOCAL_BUFFER USING OBJECT SUBOBJECT NR_RANGE_NR TOYEAR
  " QUANTITY RETURNCODE NUMBER H_EXIT.
  " IF H_EXIT = YES.
  " EXIT.
  " ELSE.
  " PERFORM READ_NRIV USING OBJECT SUBOBJECT NR_RANGE_NR TOYEAR
  " QUANTITY RETURNCODE NUMBER.
  " ENDIF.
  " ENDIF.
  " if ( tnro-buffer = local or tnro-buffer = process ).
  "* Local buffering with logical PID?
  " if tnro-buffer = process.
  "* C call for the determination of the logical WP-ID number:
  "* .. WP -> logical WP ID
  "* .. PID -> OS (Unix) WP ID
  " opcode = 8.
  "* Determine logical WP id..
  " call 'ThWpInfo' id 'OPCODE' field opcode
  " id 'WP' field wp
  " id 'PID' field pid.
  "* String concatenation for new Update argument..
  " h_offset = strlen( z_db_group_key ).
  " h_length = 19 - h_offset.
  " z_db_group_key+h_offset(1) = '_'.
  " h_offset = h_offset + 1.
  "* Content of NRIV_LOCAL field instance enhanced with WP logical ID.
  " z_db_group_key+h_offset(h_length) = wp.
  " endif. "if tnro-buffer = process.
  "* Die Intervalle stehen auf einer lokalen DB-Tabelle zur Verfügung
  " PERFORM LOCAL_NRIV USING OBJECT SUBOBJECT NR_RANGE_NR TOYEAR
  "* %_DB_GROUP_KEY RETURNCODE NUMBER.
  " z_db_group_key returncode number.
  " ENDIF.
  " if tnro-buffer = shadow.
  " call function 'TH_GET_OWN_WP_NO'
  " IMPORTING
  "* SUBRC =
  "* WP_NO =
  "* WP_PID =
  " WP_INDEX = wpindex.
  " WP_INDEX = wpindex.
  " do.
  " if wp_index(1) ca ' '.
  " shift wp_index left.
  " else.
  " exit.
  " endif.
  " enddo.

  "* opcode = 8.
  "* call 'ThWpInfo' id 'OPCODE' field opcode
  "* id 'WP' field wp
  "* id 'PID' field pid.
  "* String concatenation for new Update argument..
  " h_offset = strlen( z_db_group_key ).
  "* if at a later moment a problem should appear with the common part
  "* the instance name can be got with a th_* function module directly
  "* h_length = 22 - h_offset.
  " shadow_key = z_db_group_key.
  " shadow_key+h_offset(1) = '_'.
  " h_offset = h_offset + 1.
  "* Content of NRIV_LOCAL field instance enhanced with WP logical ID.
  " shadow_key+h_offset = wp_index.


  " PERFORM SHADOW_NRIV USING OBJECT SUBOBJECT NR_RANGE_NR TOYEAR
  " shadow_key returncode number ignore_buffer.

  " endif.

ENDFUNCTION.
