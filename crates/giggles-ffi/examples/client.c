#include "giggles.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static char *call(gd_handle h,const char *json,uint32_t code) {
    gd_handle b=0;size_t n=0;char *out;
    assert(gd_v1_call(h,(const uint8_t*)json,strlen(json),&b)==code);
    assert(b!=0);assert(gd_v1_buffer_copy(b,NULL,0,&n)==20);
    out=(char*)malloc(n+1);assert(out);assert(gd_v1_buffer_copy(b,(uint8_t*)out,n,&n)==0);out[n]=0;
    assert(gd_v1_close(b)==0);assert(gd_v1_close(b)==18);puts(out);return out;
}
static void action(gd_handle h,const char *json,uint32_t code){char *s=call(h,json,code);free(s);}
static void sql_workflow(gd_handle a,gd_handle e){
    gd_handle s=0,bad=99;char *r;
    assert(gd_v1_sql_session(e,&bad)==18);assert(bad==0);
    assert(gd_v1_sql_session(a,&s)==0);
    action(s,"{\"sql\":\"BEGIN\"}",0);
    action(s,"{\"sql\":\"CREATE TABLE actual_c (tenant BIGINT,id BIGINT,body TEXT,raw BYTEA,PRIMARY KEY(tenant,id))\"}",0);
    action(s,"{\"sql\":\"INSERT INTO actual_c VALUES($1,$2,$3,$4)\",\"params\":[{\"Int\":42},{\"Int\":-3},{\"Text\":\"typed C\"},{\"Bytes\":[0,255]}]}",0);
    action(s,"{\"sql\":\"INSERT INTO actual_c VALUES(42,9,'second',NULL)\"}",0);
    action(s,"{\"sql\":\"COMMIT\"}",0);
    r=call(s,"{\"sql\":\"SELECT id,body,raw FROM actual_c WHERE tenant=42 ORDER BY id DESC LIMIT 2\"}",0);
    assert(strstr(r,"\"Int\":9")<strstr(r,"\"Int\":-3"));assert(strstr(r,"\"Text\":\"typed C\""));assert(strstr(r,"\"Bytes\":[0,255]"));free(r);
    action(s,"{\"sql\":\"BEGIN\"}",0);action(s,"{\"sql\":\"DELETE FROM actual_c WHERE tenant=42 AND id=-3\"}",0);
    action(s,"{\"sql\":\"INSERT INTO actual_c VALUES(NULL,2,'bad',NULL)\"}",207);action(s,"{\"sql\":\"COMMIT\"}",215);action(s,"{\"sql\":\"ROLLBACK\"}",0);
    action(s,"{\"sql\":\"DELETE FROM actual_c WHERE tenant=42\"}",210);
    action(s,"{\"sql\":\"SELECT * FROM actual_c; COMMIT\"}",200);
    action(s,"{\"sql\":\"BEGIN\"}",0);action(s,"{\"sql\":\"DELETE FROM actual_c WHERE tenant=42 AND id=9\"}",0);
    assert(gd_v1_close(s)==0);
}
static gd_handle open_db(const char *mode,const char *path,const char *source){
    char json[4096];gd_handle h=0;
    if(source)snprintf(json,sizeof json,"{\"mode\":\"%s\",\"path\":\"%s\",\"export\":\"%s\"}",mode,path,source);
    else snprintf(json,sizeof json,"{\"mode\":\"%s\",\"path\":\"%s\"}",mode,path);
    assert(gd_v1_admin_open((const uint8_t*)json,strlen(json),&h)==0);return h;
}
int main(int argc,char **argv){
    gd_handle a,e=0,bad=99;char *p,*start,*end;char json[16384];
    const char *request="{\"operation\":1,\"selection\":{\"Keys\":[{\"tenant\":42,\"row\":0}]},\"column\":\"Status\",\"after\":\"Queued\"}";
    assert(argc==4);assert(gd_v1_abi_version()==1);
    assert(gd_v1_admin_open(NULL,1,&bad)==18);assert(bad==0);a=open_db("create",argv[1],NULL);
    action(a,"{\"command\":\"seed\",\"key\":{\"tenant\":42,\"row\":0},\"row\":{\"status\":\"Failed\",\"failed_at\":103,\"attempt_count\":0,\"version\":99}}",0);
    action(a,"{\"command\":\"grant\",\"grant\":{\"task\":1,\"issuer\":1,\"executor\":2,\"approver\":3,\"policy\":{\"tenant\":42,\"columns\":[\"Status\"],\"transitions\":[[\"Failed\",\"Queued\"]],\"incident\":[100,107],\"distinct_limit\":1,\"transition_limit\":1,\"approval_above\":0,\"valid_from\":1,\"expires\":1000}}}",0);
    action(a,"{\"command\":\"provision_task_resources\",\"task\":1,\"limits\":{\"read_rows\":1000,\"disclosed_rows\":100,\"encoded_bytes\":18446744073709551615}}",0);
    action(a,"{\"command\":\"provision_task_resources\",\"task\":1,\"limits\":{\"read_rows\":1000,\"disclosed_rows\":100,\"encoded_bytes\":18446744073709551615}}",102);
    p=call(a,"{\"command\":\"task_resources\",\"task\":1}",0);assert(strstr(p,"18446744073709551615"));free(p);
    assert(gd_v1_executor(a,1,2,&e)==0);action(e,"{\"command\":\"revoke\",\"task\":1}",17);
    snprintf(json,sizeof json,"{\"command\":\"preview\",\"request\":%s,\"time\":{\"Certified\":10}}",request);
    p=call(e,json,0);start=strstr(p,"\"preview\":\"");assert(start);start+=11;end=strchr(start,'"');assert(end);*end=0;
    snprintf(json,sizeof json,"{\"command\":\"approve\",\"principal\":3,\"preview\":\"%s\",\"time\":{\"Certified\":10}}",start);action(a,json,0);free(p);
    snprintf(json,sizeof json,"{\"command\":\"execute\",\"request\":%s,\"time\":{\"Certified\":10},\"approval\":0}",request);action(e,json,0);action(e,json,0);
    action(e,"{\"command\":\"status\",\"operation\":1}",0);action(e,"{\"command\":\"status\",\"operation\":99}",1);
    assert(gd_v1_executor(e,1,2,&bad)==18);assert(bad==0);
    action(e,"{\"command\":\"provision_task_resources\",\"task\":1,\"limits\":{\"read_rows\":1000,\"disclosed_rows\":100,\"encoded_bytes\":1000}}",17);
    sql_workflow(a,e);
    snprintf(json,sizeof json,"{\"command\":\"export\",\"path\":\"%s\"}",argv[2]);action(a,json,0);action(a,json,13);
    assert(gd_v1_close(e)==0);assert(gd_v1_close(a)==0);
    a=open_db("open",argv[1],NULL);assert(gd_v1_executor(a,1,2,&e)==0);action(e,"{\"command\":\"status\",\"operation\":1}",0);
    action(a,"{\"command\":\"revoke\",\"task\":1}",0);
    assert(gd_v1_close(e)==0);assert(gd_v1_close(a)==0);
    a=open_db("restore",argv[3],argv[2]);assert(gd_v1_executor(a,1,2,&e)==0);action(e,"{\"command\":\"status\",\"operation\":1}",0);
    action(e,"{\"command\":\"execute\",\"request\":{\"operation\":2,\"selection\":{\"Keys\":[{\"tenant\":42,\"row\":0}]},\"column\":\"Status\",\"after\":\"Queued\"},\"time\":{\"Certified\":11},\"approval\":null}",104);
    assert(gd_v1_close(e)==0);assert(gd_v1_close(a)==0);assert(gd_v1_close(a)==18);
    puts("C client passed: approval, retry, authority, restart, export, fenced restore");return 0;
}
