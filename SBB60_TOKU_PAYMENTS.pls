create or replace package body sbb60_toku_payments is

  w_version_seq_no icom_interface_version.version_seq_no%type;
  w_version_parm   b8350.gt_version_parameters;
  w_job_id         integer;
  w_record_no      number := 0;
  w_total_amt      number := 0;

  procedure main(p in out p8460.batch_parameter_record) is
  
  begin
    null;
  end main;

  procedure main(p_operation varchar2) is
  
  begin
    w_record_no                   := 0;
    w_total_amt                   := 0;
    w_version_parm.interface_name := 'TOKU_PAYMENTS';
  
    if p_operation = 'EXPORT' then
      --sbb60_uf_payments.register_batch(p_operation_type => p_operation);
      --g_batch_no := t6305.rec.batch_no;
    
      w_version_parm.operation_name        := 'EXPORT_PAYMENTS';
      w_version_parm.version_seq_no        := null;
      w_version_parm.earliest_release_time := null;
    
      w_version_seq_no := b8350.find_version(p_trans_book_seq_no => null, p_version_parameters => w_version_parm);
      w_job_id         := b8350.create_job(w_version_seq_no, null);
      b8350.execute_job(w_job_id, p0000.get_site_seq_no);
    
      t6305.clr; 
    
      t6305.rec.batch_no := g_batch_no;
      t6305.rec.number_of_items  := nvl(w_record_no, 0);
      t6305.rec.number_of_errors := nvl(b6300.no_of_errors, 0);
      t6305.rec.total_amount     := nvl(w_total_amt * 0.01, 0);
    
      t6305.items_changed := 'number_of_items,number_of_errors,total_amount,';
      t6305.upd;
    else
      
      --sbb60_uf_payments.register_batch(p_operation_type => p_operation);
      --g_batch_no := t6305.rec.batch_no;
      
      g_batch_no :=7777;
      w_version_parm.operation_name        := 'IMPORT PAYMENTS';
      w_version_parm.version_seq_no        := null;
      w_version_parm.earliest_release_time := null;
    
      w_version_seq_no := b8350.find_version(p_trans_book_seq_no => null, p_version_parameters => w_version_parm);
      do_log('MA w_version_seq_no: ' || w_version_seq_no);
      w_job_id := b8350.create_job(w_version_seq_no, null);
    do_log('MA w_job_id: ' || w_job_id);
      b8350.execute_job(w_job_id, p0000.get_site_seq_no);
      do_log('MA after execute job');
      /*t6305.clr;
    
      t6305.rec.batch_no         := g_batch_no;
      t6305.rec.number_of_items  := nvl(w_record_no, 0);
      t6305.rec.number_of_errors := nvl(b6300.no_of_errors, 0);
      t6305.rec.total_amount     := nvl(w_total_amt, 0);
    
      t6305.items_changed := 'number_of_items,number_of_errors,total_amount,';
      t6305.upd;*/
    end if;
      exception
    when others then
      do_log('MA error: '||dbms_utility.format_error_backtrace);
      z_error_handle;
  end main;

  procedure feedsa_export is
  
    w_prod_id      varchar2(2);
    w_prod_desc    varchar2(50);
    w_name_id      number;
    w_contact_info varchar2(100);
    w_name         varchar2(32);
    w_surname      varchar2(200);
    w_line_no      number;
  
    cursor cur_get_payments(c_in_start_date  date,
                            c_in_end_date    date,
                            c_in_pay_method  varchar2,
                            c_in_pay_channel varchar2) is
    
      select p.acc_payment_no,
             p.payment_method,
             p.payment_channel,
             p.receiver_id,
             p.means_pay_no,
             p.due_date,
             p.currency_code,
             p.amt,
             p.payment_details,
             i.currency_amt,
             i.name_id,
             i.policy_no,
             i.n03
      from   acc_payment p
      inner  join acc_payment_item pi
      on     p.acc_payment_no = pi.acc_payment_no
      inner  join acc_item i
      on     pi.acc_item_no = i.acc_item_no
      where  batch_out is null
      and    p.payment_status = 3
      and    trunc(p.due_date) >= trunc(to_date('10-08-2023', 'dd-mm-yyyy'))
      and    trunc(p.due_date) <= trunc(to_date('10-08-2023', 'dd-mm-yyyy'))
      and    p.payment_method = c_in_pay_method
      and    p.payment_channel = c_in_pay_channel
      and    i.source = 7
      and    i.currency_balance <> 0
      and    i.item_class = 1
      order  by i.name_id;
  
    cursor cur_get_prod_info(c_in_policy_no policy_entity.policy_no%type) is
      select p.prod_id,
             p.prod_desc_1
      from   policy_entity pe,
             product       p
      where  pe.prod_id = p.prod_id
      and    pe.policy_no = c_in_policy_no;
  
    cursor cur_get_name_info(c_in_name_id name.id_no%type) is
      select n.name,
             n.surname,
             listagg(nt.phone_no, ',') within group(order by nt.telephone_type)
      from   name           n,
             name_telephone nt
      where  n.id_no = nt.name_id_no
      and    n.id_no = c_in_name_id
      and    telephone_type in ('01', '04')
      group  by nt.name_id_no,
                n.name,
                n.surname;
  
  begin
    w_record_no := 0;
    w_total_amt := 0;
    for r in cur_get_payments(c_in_start_date => sysdate, c_in_end_date => sysdate, c_in_pay_method => 'CUPONERA', c_in_pay_channel => 'TOKU')
    loop
    
      open cur_get_prod_info(c_in_policy_no => r.policy_no);
      fetch cur_get_prod_info
        into w_prod_id,
             w_prod_desc;
      close cur_get_prod_info;
    
      open cur_get_name_info(c_in_name_id => r.name_id);
      fetch cur_get_name_info
        into w_name,
             w_surname,
             w_contact_info;
      close cur_get_name_info;
    
      /*     insert into sbt60_payment_staging_out
        (payment_channel,
         batch_no,
         client_id,
         client_name,
         client_sur_nmame,
         client_email,
         client_phone_no,
         product_id,
         policy_no,
         prod_desc,
         payment_no,
         instl_no,
         due_date,
         currency_code,
         amount)
      values
        ('TOKU',
         5555,
         r.name_id,
         w_name,
         w_surname,
         regexp_substr(w_contact_info, '[^,]+', 1, 2),
         regexp_substr(w_contact_info, '[^,]+', 1, 1),
         w_prod_id,
         r.policy_no,
         w_prod_desc,
         r.acc_payment_no,
         r.n03, --instl_no
         r.due_date,
         r.currency_code,
         r.currency_amt);*/
      w_total_amt := w_total_amt + r.currency_amt;
      w_record_no := w_record_no + 1;
    end loop;
  
    b8351.clear_record;
  
    b8351.add_field(1, trunc(sysdate));
    b8351.add_field(2, 'Export Toku');
    b8351.add_field(3, 5555);
    b8351.add_field(4, 'P');
    b8351.add_field(5, w_record_no);
    b8351.add_field(6, w_total_amt);
    w_line_no := b8351.add_record(b8350.gw_current_version_parameters, 'S');
  
    commit;
  end feedsa_export;




  function get_ff return clob is
  
    w_program varchar2(200) := p0000.program;
  
    cursor cur_get_payment_data is
      select *
      from   sbt60_payment_staging_out
      where  batch_no = 5555;
  
  begin
    z_program('get_ff');
    z_trace(w_program);
    b8353.trans_book_trace('Intro Compose Payload. Line=' || b8350.gw_staging_record.line_no, 20, 'S', b8350.gw_staging_record.line_no);
    w_record_no := 0;
    w_total_amt := 0;
  
    for r in cur_get_payment_data
    loop
      w_total_amt := w_total_amt + r.amount;
      w_record_no := w_record_no + 1;
      w_payload   := w_payload || r.payment_channel || ';' || r.batch_no || ';' || r.client_id || ';' || r.client_name || ';' ||
                     r.client_sur_nmame || ';' || r.client_email || ';' || r.client_phone_no || ';' || r.product_id || ';' ||
                     r.policy_no || ';' || r.prod_desc || ';' || r.payment_no || ';' || r.instl_no || ';' || r.due_date || ';' ||
                     r.currency_code || ';' || r.currency_code || ';' || r.amount || ';' || chr(10);
    end loop;
  
    return w_payload;
  exception
    when others then
      z_error_handle;
    
  end get_ff;

  procedure crt_stg is
  
    w_parms b8350.gt_version_parameters;
    w_line_no      number;
  
  begin
    z_program('ta.crt_stg');
    z_trace('ta.crt_stg');
  DO_LOG('MA crt_stg');
    b8351.clear_record;
  
    b8351.add_field(1, trunc(sysdate));
    b8351.add_field(2, 'Export Toku');
    b8351.add_field(3, 5555);
    b8351.add_field(4, 7777);
    b8351.add_field(5, 'P');
    b8351.add_field(6, 10);
    b8351.add_field(7, 100);
  
    --b8350.gw_line_payload := b8350.gw_current_payload;
    w_line_no := b8351.add_record(b8350.gw_current_version_parameters, 'R');
  
   -- b8350.gw_line_no := b8351.add_record(w_parms, 'R');
    b8353.trans_book_trace('Have added record. Line no:' || b8350.gw_line_no, 15, 'R', b8350.gw_line_no);
  
    commit;
  
    --b6300.no_of_items  := 10;
    --b6300.total_amount := 100;
  
  exception
    when others then
      z_error_handle;
  end crt_stg;
  
  PROCEDURE upd_data is 
    begin
      DO_LOG('MA upd_data');
      sbb60_uf_payments.create_items_for_imported_payments;
   end upd_data;

end sbb60_toku_payments;
