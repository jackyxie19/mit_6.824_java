package base;

import java.util.List;

public interface Namespace {
    String getFullPath();

    boolean isRoot();

    boolean hasChild();

    Namespace getParent();

    List<Namespace> getChildren();
}
