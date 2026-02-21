args <- commandArgs(trailingOnly=TRUE)
csv_path <- Sys.getenv("GOLD_FEATURES_CSV")
if (csv_path == "") stop("Set GOLD_FEATURES_CSV env var")

df <- read.csv(csv_path)
df[is.na(df)] <- 0

y <- df$avg_delay_seconds
X <- subset(df, select = -c(avg_delay_seconds))

# Simple baseline: linear regression
model <- lm(y ~ ., data = cbind(y=y, X))

dir.create("artifacts", showWarnings = FALSE)
saveRDS(model, "artifacts/model_r.rds")

cat("Trained R baseline model and saved to artifacts/model_r.rds\n")
