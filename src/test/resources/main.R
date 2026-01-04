library("rpart")
library("r2pmml")

data("iris")

iris.rpart = rpart(Species ~ ., data = iris, control = rpart.control(usesurrogate = 0))
iris.rpart$variable.importance = NULL

r2pmml(iris.rpart, "DecisionTreeIris.pmml")

iris.features = iris[, 1:4]
write.csv(iris.features, "Iris.csv", row.names = FALSE, quote = FALSE)

invalid_rows = c(1, 51, 101)
iris.features[invalid_rows, ] = NA
write.csv(iris.features, "IrisInvalid.csv", row.names = FALSE, quote = FALSE, na = "NaN")
