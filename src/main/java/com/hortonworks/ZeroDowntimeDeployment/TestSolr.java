package com.hortonworks.ZeroDowntimeDeployment;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.hortonworks.ZeroDowntimeDeployment.Utils.Configs;

@SuppressWarnings("deprecation")
public class TestSolr {

	public static void main(String[] args) {

		HttpSolrServer server = new HttpSolrServer(Configs.solr_server_cloud);

		SolrInputDocument doc = new SolrInputDocument();
		
		String msgId = args[0];
		String msgAssId = args[1];
		
		doc.addField("id", msgId);
		doc.addField("accessid", msgAssId);
		try {
			server.add(doc);
			server.commit(false, false);
			System.out
					.println("SolrBolt: successfully added tweet to Solr server");
		} catch (Exception e) {
			e.printStackTrace();
			// collector.fail(input);
		}

	}

}
