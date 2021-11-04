# Databricks notebook source
configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
          "fs.adl.oauth2.client.id": "xxxxxxxxxxxxxxxxxxxxxxx",
          "fs.adl.oauth2.credential": "xxxxxxxxxxxxxxxxxxxxxxxxxxx",
          "fs.adl.oauth2.refresh.url": "https://login.microsoftonline.com/<tenant_id>/oauth2/token"}
 
dbutils.fs.mount(
  source = "adl://<adls_acct_name>.azuredatalakestore.net/",
  mount_point = "/mnt/<mnt_point_name>",
  extra_configs = configs)
