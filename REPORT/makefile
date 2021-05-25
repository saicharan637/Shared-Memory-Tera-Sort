J = javac
.SUFFIXES: .java .class
.java.class:
	$(J) *.java

CLASSES = \
	MySort.java

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

