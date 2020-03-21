package com.github.harbby.ashtarte;

import com.github.harbby.ashtarte.api.Stage;
import com.github.harbby.gadtry.graph.Graph;

import java.util.Map;

public class GraphScheduler {
    private final MppContext mppContext;

    public GraphScheduler(MppContext mppContext) {
        this.mppContext = mppContext;
    }

    public void runGraph(Map<Stage, Integer[]> stages) {
        Graph.GraphBuilder<Stage, Void> builder = Graph.builder();
        for (Stage stage : stages.keySet()) {
            builder.addNode(stage.getStageId() + "", stage);
        }

        for (Map.Entry<Stage, Integer[]> entry : stages.entrySet()) {
            for (int id : entry.getValue()) {
                builder.addEdge(id + "", entry.getKey().getStageId() + "");
            }

        }
        Graph<Stage, Void> graph = builder.create();
        graph.printShow().forEach(x-> System.out.println(x));
    }
}
