package edu.cmu.pdl.macaronsimulator.simulator.message;

import java.util.List;

public class ClusterInfoContent implements Content {
    List<String> proxyEngineNameList = null;

    public ClusterInfoContent(final List<String> proxyEngineNameList) {
        this.proxyEngineNameList = proxyEngineNameList;
    }

    public List<String> getCacheEngineNameList() {
        assert proxyEngineNameList != null;
        return proxyEngineNameList;
    }

    @Override
    public ContentType getContentType() {
        return ContentType.CLUSTER_INFO;
    }
}
