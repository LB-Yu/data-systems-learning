package org.apache.hadoop.yarn.applications.distributedshell;

import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParseException;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser;
import org.apache.hadoop.yarn.util.constraint.PlacementConstraintParser.SourceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class encapsulating a SourceTag, number of container and a Placement
 * Constraint.
 */
public class PlacementSpec {

    private static final Logger LOG =
            LoggerFactory.getLogger(PlacementSpec.class);

    public final String sourceTag;
    public final PlacementConstraint constraint;
    private int numContainers;

    public PlacementSpec(String sourceTag, int numContainers,
                         PlacementConstraint constraint) {
        this.sourceTag = sourceTag;
        this.numContainers = numContainers;
        this.constraint = constraint;
    }

    /**
     * Get the number of container for this spec.
     * @return container count
     */
    public int getNumContainers() {
        return numContainers;
    }

    /**
     * Set number of containers for this spec.
     * @param numContainers number of containers.
     */
    public void setNumContainers(int numContainers) {
        this.numContainers = numContainers;
    }

    // Placement specification should be of the form:
    // PlacementSpec => ""|KeyVal;PlacementSpec
    // KeyVal => SourceTag=Constraint
    // SourceTag => String
    // Constraint => NumContainers|
    //               NumContainers,"in",Scope,TargetTag|
    //               NumContainers,"notin",Scope,TargetTag|
    //               NumContainers,"cardinality",Scope,TargetTag,MinCard,MaxCard
    // NumContainers => int (number of containers)
    // Scope => "NODE"|"RACK"
    // TargetTag => String (Target Tag)
    // MinCard => int (min cardinality - needed if ConstraintType == cardinality)
    // MaxCard => int (max cardinality - needed if ConstraintType == cardinality)

    /**
     * Parser to convert a string representation of a placement spec to mapping
     * from source tag to Placement Constraint.
     *
     * @param specs Placement spec.
     * @return Mapping from source tag to placement constraint.
     */
    public static Map<String, PlacementSpec> parse(String specs)
            throws IllegalArgumentException {
        LOG.info("Parsing Placement Specs: [{}]", specs);

        Map<String, PlacementSpec> pSpecs = new HashMap<>();
        Map<SourceTags, PlacementConstraint> parsed;
        try {
            parsed = PlacementConstraintParser.parsePlacementSpec(specs);
            for (Map.Entry<SourceTags, PlacementConstraint> entry :
                    parsed.entrySet()) {
                LOG.info("Parsed source tag: {}, number of allocations: {}",
                        entry.getKey().getTag(), entry.getKey().getNumOfAllocations());
                if (entry.getValue() != null) {
                    LOG.info("Parsed constraint: {}", entry.getValue()
                            .getConstraintExpr().getClass().getSimpleName());
                } else {
                    LOG.info("Parsed constraint Empty");
                }
                pSpecs.put(entry.getKey().getTag(), new PlacementSpec(
                        entry.getKey().getTag(),
                        entry.getKey().getNumOfAllocations(),
                        entry.getValue()));
            }
            return pSpecs;
        } catch (PlacementConstraintParseException e) {
            throw new IllegalArgumentException(
                    "Invalid placement spec: " + specs, e);
        }
    }
}
