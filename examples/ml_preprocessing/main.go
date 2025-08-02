// Package main demonstrates machine learning preprocessing patterns with Gorilla DataFrame.
//
// This example showcases:
// - Feature engineering and transformation
// - Data normalization and scaling
// - Categorical encoding techniques
// - Train/validation/test splits
// - Missing value imputation
// - Outlier detection and handling
// - Feature selection and dimensionality reduction
//
//nolint:gosec,mnd // Example code: random generation and magic numbers acceptable for demo
package main

import (
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

func main() {
	fmt.Println("=== Machine Learning Preprocessing Example ===")

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Generate sample dataset for ML preprocessing
	rawData := generateMLDataset(mem)
	defer rawData.Release()

	fmt.Printf("Raw dataset: %d samples, %d features\n", rawData.Len(), rawData.Width())

	// Step 1: Data quality analysis and cleaning
	fmt.Println("\n1. Data Quality Analysis...")
	qualityReport := analyzeDataQuality(rawData)
	defer qualityReport.Release()

	fmt.Println("Data Quality Report:")
	fmt.Println(qualityReport)

	// Step 2: Feature engineering
	fmt.Println("\n2. Feature Engineering...")
	engineeredData := performFeatureEngineering(rawData)
	defer engineeredData.Release()

	fmt.Printf("After feature engineering: %d samples, %d features\n",
		engineeredData.Len(), engineeredData.Width())

	// Step 3: Handle missing values and outliers
	fmt.Println("\n3. Data Cleaning and Imputation...")
	cleanedData := cleanAndImputeData(engineeredData)
	defer cleanedData.Release()

	// Step 4: Feature scaling and normalization
	fmt.Println("\n4. Feature Scaling...")
	scaledData := scaleFeatures(cleanedData)
	defer scaledData.Release()

	// Step 5: Categorical encoding
	fmt.Println("\n5. Categorical Encoding...")
	encodedData := encodeCategoricalFeatures(scaledData)
	defer encodedData.Release()

	// Step 6: Feature selection
	fmt.Println("\n6. Feature Selection...")
	selectedFeatures := selectFeatures(encodedData)
	defer selectedFeatures.Release()

	fmt.Printf("After feature selection: %d samples, %d features\n",
		selectedFeatures.Len(), selectedFeatures.Width())

	// Step 7: Train/validation/test split
	fmt.Println("\n7. Dataset Splitting...")
	trainSet, validSet, testSet := splitDataset(selectedFeatures)
	defer trainSet.Release()
	defer validSet.Release()
	defer testSet.Release()

	fmt.Printf("Train: %d samples, Validation: %d samples, Test: %d samples\n",
		trainSet.Len(), validSet.Len(), testSet.Len())

	// Step 8: Final preprocessing summary
	fmt.Println("\n8. Preprocessing Summary...")
	summary := generatePreprocessingSummary(trainSet)
	defer summary.Release()

	fmt.Println("Final Training Set Summary:")
	fmt.Println(summary)
}

// generateMLDataset creates a realistic ML dataset with various data types and quality issues.
//
//nolint:gocognit,gosec,mnd,funlen // Example code: complexity and random generation acceptable for demo
func generateMLDataset(mem memory.Allocator) *gorilla.DataFrame {
	const numSamples = 1000

	// Generate features with different characteristics
	ids := make([]int64, numSamples)
	ages := make([]int64, numSamples)
	incomes := make([]float64, numSamples)
	education := make([]string, numSamples)
	experience := make([]float64, numSamples)
	creditScore := make([]int64, numSamples)
	categories := make([]string, numSamples)
	targets := make([]int64, numSamples) // Binary classification target

	educationLevels := []string{"High School", "Bachelor", "Master", "PhD", ""}
	categoryTypes := []string{"A", "B", "C", "D"}

	for i := range numSamples {
		ids[i] = int64(i + 1)

		// Age with some missing values (represented as -1)
		if rand.Float32() < 0.05 {
			ages[i] = -1 // Missing value
		} else {
			ages[i] = int64(22 + rand.IntN(43)) // 22-65
		}

		// Income with outliers and missing values
		//nolint:gocritic // Example code: if-else chain acceptable for demo
		if rand.Float32() < 0.03 {
			incomes[i] = -1.0 // Missing value
		} else if rand.Float32() < 0.02 {
			incomes[i] = 500000 + rand.Float64()*1000000 // Outliers
		} else {
			incomes[i] = 30000 + rand.Float64()*120000 // Normal range
		}

		// Education with missing values
		if rand.Float32() < 0.08 {
			education[i] = ""
		} else {
			education[i] = educationLevels[rand.IntN(len(educationLevels)-1)]
		}

		// Experience (years) correlated with age
		if ages[i] > 0 {
			experience[i] = math.Max(0, float64(ages[i]-22)) + rand.NormFloat64()*2
		} else {
			experience[i] = -1.0 // Missing when age is missing
		}

		// Credit score with some missing values
		if rand.Float32() < 0.04 {
			creditScore[i] = -1
		} else {
			creditScore[i] = int64(300 + rand.IntN(551)) // 300-850
		}

		categories[i] = categoryTypes[rand.IntN(len(categoryTypes))]

		// Generate target variable with some logic
		score := 0.0
		if ages[i] > 0 {
			score += float64(ages[i]) * 0.1
		}
		if incomes[i] > 0 {
			score += incomes[i] * 0.00001
		}
		if creditScore[i] > 0 {
			score += float64(creditScore[i]) * 0.01
		}

		targets[i] = int64(0)
		if score > 50 && rand.Float32() < 0.7 {
			targets[i] = 1
		} else if score <= 50 && rand.Float32() < 0.3 {
			targets[i] = 1
		}
	}

	// Create series
	idSeries := gorilla.NewSeries("id", ids, mem)
	ageSeries := gorilla.NewSeries("age", ages, mem)
	incomeSeries := gorilla.NewSeries("income", incomes, mem)
	educationSeries := gorilla.NewSeries("education", education, mem)
	experienceSeries := gorilla.NewSeries("experience", experience, mem)
	creditSeries := gorilla.NewSeries("credit_score", creditScore, mem)
	categorySeries := gorilla.NewSeries("category", categories, mem)
	targetSeries := gorilla.NewSeries("target", targets, mem)

	defer idSeries.Release()
	defer ageSeries.Release()
	defer incomeSeries.Release()
	defer educationSeries.Release()
	defer experienceSeries.Release()
	defer creditSeries.Release()
	defer categorySeries.Release()
	defer targetSeries.Release()

	return gorilla.NewDataFrame(
		idSeries, ageSeries, incomeSeries, educationSeries,
		experienceSeries, creditSeries, categorySeries, targetSeries,
	)
}

// analyzeDataQuality performs comprehensive data quality analysis.
func analyzeDataQuality(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Calculate data quality metrics per category
		GroupBy("category").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("total_samples"),

			// Missing value counts
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("age").Eq(gorilla.Lit(int64(-1))),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("missing_age"),

			gorilla.Sum(
				gorilla.If(
					gorilla.Col("income").Eq(gorilla.Lit(-1.0)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("missing_income"),

			gorilla.Sum(
				gorilla.If(
					gorilla.Col("education").Eq(gorilla.Lit("")),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("missing_education"),

			// Outlier detection (simple method)
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("income").Gt(gorilla.Lit(200000.0)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("income_outliers"),

			// Basic statistics
			gorilla.Mean(
				gorilla.If(
					gorilla.Col("age").Gt(gorilla.Lit(int64(0))),
					gorilla.Col("age"),
					gorilla.Lit(int64(0)),
				),
			).As("avg_age"),
		).

		// Calculate quality percentages
		WithColumn("missing_age_pct",
			gorilla.Col("missing_age").Div(gorilla.Col("total_samples")).Mul(gorilla.Lit(100.0)),
		).
		WithColumn("missing_income_pct",
			gorilla.Col("missing_income").Div(gorilla.Col("total_samples")).Mul(gorilla.Lit(100.0)),
		).
		WithColumn("outlier_pct",
			gorilla.Col("income_outliers").Div(gorilla.Col("total_samples")).Mul(gorilla.Lit(100.0)),
		).
		Collect()

	if err != nil {
		log.Fatalf("Data quality analysis failed: %v", err)
	}

	return result
}

// performFeatureEngineering creates new features from existing ones.
func performFeatureEngineering(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Age-based features
		WithColumn("age_group",
			gorilla.Case().
				When(gorilla.Col("age").Lt(gorilla.Lit(int64(0))), gorilla.Lit("Unknown")).
				When(gorilla.Col("age").Lt(gorilla.Lit(int64(30))), gorilla.Lit("Young")).
				When(gorilla.Col("age").Lt(gorilla.Lit(int64(45))), gorilla.Lit("Middle")).
				When(gorilla.Col("age").Lt(gorilla.Lit(int64(60))), gorilla.Lit("Senior")).
				Else(gorilla.Lit("Elder")),
		).

		// Income-based features
		WithColumn("income_tier",
			gorilla.Case().
				When(gorilla.Col("income").Lt(gorilla.Lit(0.0)), gorilla.Lit("Unknown")).
				When(gorilla.Col("income").Lt(gorilla.Lit(40000.0)), gorilla.Lit("Low")).
				When(gorilla.Col("income").Lt(gorilla.Lit(80000.0)), gorilla.Lit("Medium")).
				When(gorilla.Col("income").Lt(gorilla.Lit(150000.0)), gorilla.Lit("High")).
				Else(gorilla.Lit("Very High")),
		).

		// Experience-based features
		WithColumn("experience_level",
			gorilla.Case().
				When(gorilla.Col("experience").Lt(gorilla.Lit(0.0)), gorilla.Lit("Unknown")).
				When(gorilla.Col("experience").Lt(gorilla.Lit(2.0)), gorilla.Lit("Entry")).
				When(gorilla.Col("experience").Lt(gorilla.Lit(5.0)), gorilla.Lit("Junior")).
				When(gorilla.Col("experience").Lt(gorilla.Lit(10.0)), gorilla.Lit("Mid")).
				Else(gorilla.Lit("Senior")),
		).

		// Ratio features
		WithColumn("experience_per_age",
			gorilla.If(
				gorilla.Col("age").Gt(gorilla.Lit(int64(0))),
				gorilla.Col("experience").Div(gorilla.Col("age")),
				gorilla.Lit(0.0),
			),
		).

		// Credit score categories
		WithColumn("credit_grade",
			gorilla.Case().
				When(gorilla.Col("credit_score").Lt(gorilla.Lit(int64(0))), gorilla.Lit("Unknown")).
				When(gorilla.Col("credit_score").Lt(gorilla.Lit(int64(580))), gorilla.Lit("Poor")).
				When(gorilla.Col("credit_score").Lt(gorilla.Lit(int64(670))), gorilla.Lit("Fair")).
				When(gorilla.Col("credit_score").Lt(gorilla.Lit(int64(740))), gorilla.Lit("Good")).
				When(gorilla.Col("credit_score").Lt(gorilla.Lit(int64(800))), gorilla.Lit("Very Good")).
				Else(gorilla.Lit("Excellent")),
		).

		// Binary flags
		WithColumn("has_degree",
			gorilla.Col("education").Eq(gorilla.Lit("Bachelor")).Or(
				gorilla.Col("education").Eq(gorilla.Lit("Master")),
			).Or(
				gorilla.Col("education").Eq(gorilla.Lit("PhD")),
			),
		).
		WithColumn("high_income",
			gorilla.Col("income").Gt(gorilla.Lit(100000.0)),
		).
		WithColumn("experienced",
			gorilla.Col("experience").Gt(gorilla.Lit(5.0)),
		).
		Collect()

	if err != nil {
		log.Fatalf("Feature engineering failed: %v", err)
	}

	return result
}

// cleanAndImputeData handles missing values and outliers.
func cleanAndImputeData(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Impute missing ages with median (approximated as 35)
		WithColumn("age_clean",
			gorilla.If(
				gorilla.Col("age").Lt(gorilla.Lit(int64(0))),
				gorilla.Lit(int64(35)), // Median age approximation
				gorilla.Col("age"),
			),
		).

		// Cap income outliers
		WithColumn("income_clean",
			gorilla.Case().
				When(gorilla.Col("income").Lt(gorilla.Lit(0.0)), gorilla.Lit(50000.0)).       // Median income
				When(gorilla.Col("income").Gt(gorilla.Lit(300000.0)), gorilla.Lit(300000.0)). // Cap outliers
				Else(gorilla.Col("income")),
		).

		// Impute missing experience based on age
		WithColumn("experience_clean",
			gorilla.If(
				gorilla.Col("experience").Lt(gorilla.Lit(0.0)),
				gorilla.If(
					gorilla.Col("age_clean").Gt(gorilla.Lit(int64(25))),
					gorilla.Col("age_clean").Sub(gorilla.Lit(int64(22))), // Assume started at 22
					gorilla.Lit(0.0),
				),
				gorilla.Col("experience"),
			),
		).

		// Impute missing credit scores with category average (approximated)
		WithColumn("credit_score_clean",
			gorilla.If(
				gorilla.Col("credit_score").Lt(gorilla.Lit(int64(0))),
				gorilla.Case().
					When(gorilla.Col("category").Eq(gorilla.Lit("A")), gorilla.Lit(int64(720))).
					When(gorilla.Col("category").Eq(gorilla.Lit("B")), gorilla.Lit(int64(680))).
					When(gorilla.Col("category").Eq(gorilla.Lit("C")), gorilla.Lit(int64(650))).
					Else(gorilla.Lit(int64(620))),
				gorilla.Col("credit_score"),
			),
		).

		// Handle missing education
		WithColumn("education_clean",
			gorilla.If(
				gorilla.Col("education").Eq(gorilla.Lit("")),
				gorilla.Lit("High School"), // Most common education level
				gorilla.Col("education"),
			),
		).

		// Create data quality flags
		WithColumn("imputed_age", gorilla.Col("age").Lt(gorilla.Lit(int64(0)))).
		WithColumn("imputed_income", gorilla.Col("income").Lt(gorilla.Lit(0.0))).
		WithColumn("capped_income", gorilla.Col("income").Gt(gorilla.Lit(300000.0))).
		Collect()

	if err != nil {
		log.Fatalf("Data cleaning failed: %v", err)
	}

	return result
}

// scaleFeatures normalizes numerical features.
func scaleFeatures(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Min-max scaling for age (assuming min=22, max=65)
		WithColumn("age_scaled",
			gorilla.Col("age_clean").Sub(gorilla.Lit(int64(22))).Div(gorilla.Lit(int64(43))),
		).

		// Log transformation for income to handle skewness
		WithColumn("income_log",
			gorilla.Col("income_clean").Add(gorilla.Lit(1.0)).Log(),
		).

		// Z-score normalization for experience (approximating mean=10, std=8)
		WithColumn("experience_normalized",
			gorilla.Col("experience_clean").Sub(gorilla.Lit(10.0)).Div(gorilla.Lit(8.0)),
		).

		// Min-max scaling for credit score (300-850 range)
		WithColumn("credit_score_scaled",
			gorilla.Col("credit_score_clean").Sub(gorilla.Lit(int64(300))).Div(gorilla.Lit(int64(550))),
		).
		Collect()

	if err != nil {
		log.Fatalf("Feature scaling failed: %v", err)
	}

	return result
}

// encodeCategoricalFeatures converts categorical variables to numerical.
func encodeCategoricalFeatures(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// One-hot encoding for education
		WithColumn("edu_high_school",
			gorilla.If(
				gorilla.Col("education_clean").Eq(gorilla.Lit("High School")),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).
		WithColumn("edu_bachelor",
			gorilla.If(
				gorilla.Col("education_clean").Eq(gorilla.Lit("Bachelor")),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).
		WithColumn("edu_master",
			gorilla.If(
				gorilla.Col("education_clean").Eq(gorilla.Lit("Master")),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).
		WithColumn("edu_phd",
			gorilla.If(
				gorilla.Col("education_clean").Eq(gorilla.Lit("PhD")),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).

		// Label encoding for category (A=1, B=2, C=3, D=4)
		WithColumn("category_encoded",
			gorilla.Case().
				When(gorilla.Col("category").Eq(gorilla.Lit("A")), gorilla.Lit(int64(1))).
				When(gorilla.Col("category").Eq(gorilla.Lit("B")), gorilla.Lit(int64(2))).
				When(gorilla.Col("category").Eq(gorilla.Lit("C")), gorilla.Lit(int64(3))).
				Else(gorilla.Lit(int64(4))),
		).

		// Convert boolean features to integers
		WithColumn("has_degree_int",
			gorilla.If(
				gorilla.Col("has_degree").Eq(gorilla.Lit(true)),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).
		WithColumn("high_income_int",
			gorilla.If(
				gorilla.Col("high_income").Eq(gorilla.Lit(true)),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).
		WithColumn("experienced_int",
			gorilla.If(
				gorilla.Col("experienced").Eq(gorilla.Lit(true)),
				gorilla.Lit(int64(1)),
				gorilla.Lit(int64(0)),
			),
		).
		Collect()

	if err != nil {
		log.Fatalf("Categorical encoding failed: %v", err)
	}

	return result
}

// selectFeatures selects the most important features for modeling.
func selectFeatures(df *gorilla.DataFrame) *gorilla.DataFrame {
	// Select final feature set for modeling
	result := df.Select(
		"id", "target", // ID and target
		// Scaled numerical features
		"age_scaled", "income_log", "experience_normalized", "credit_score_scaled",
		// Encoded categorical features
		"edu_bachelor", "edu_master", "edu_phd", // High school is reference category
		"category_encoded",
		// Binary features
		"has_degree_int", "high_income_int", "experienced_int",
		// Data quality flags
		"imputed_age", "imputed_income", "capped_income",
	)

	return result
}

// splitDataset splits data into train/validation/test sets.
func splitDataset(df *gorilla.DataFrame) (*gorilla.DataFrame, *gorilla.DataFrame, *gorilla.DataFrame) {
	// Sort by ID for reproducible splits
	sorted, err := df.Sort("id", true)
	if err != nil {
		log.Fatalf("Sorting failed: %v", err)
	}
	defer sorted.Release()

	totalSamples := sorted.Len()

	// 70% train, 15% validation, 15% test
	trainEnd := int(float64(totalSamples) * 0.7)
	validEnd := int(float64(totalSamples) * 0.85)

	trainSet := sorted.Slice(0, trainEnd)
	validSet := sorted.Slice(trainEnd, validEnd)
	testSet := sorted.Slice(validEnd, totalSamples)

	return trainSet, validSet, testSet
}

// generatePreprocessingSummary creates a summary of the final preprocessed data.
func generatePreprocessingSummary(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Select columns for summary statistics (exclude ID)
		Select("target", "age_scaled", "income_log", "credit_score_scaled",
			"edu_bachelor", "edu_master", "edu_phd", "category_encoded").

		// Group by target class for class-wise statistics
		GroupBy("target").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("sample_count"),
			gorilla.Mean(gorilla.Col("age_scaled")).As("avg_age_scaled"),
			gorilla.Mean(gorilla.Col("income_log")).As("avg_income_log"),
			gorilla.Mean(gorilla.Col("credit_score_scaled")).As("avg_credit_scaled"),
			gorilla.Sum(gorilla.Col("edu_bachelor")).As("bachelor_count"),
			gorilla.Sum(gorilla.Col("edu_master")).As("master_count"),
			gorilla.Sum(gorilla.Col("edu_phd")).As("phd_count"),
			gorilla.Mean(gorilla.Col("category_encoded")).As("avg_category"),
		).

		// Calculate class percentages
		Collect()

	if err != nil {
		log.Fatalf("Summary generation failed: %v", err)
	}

	// Calculate total sample count by iterating through rows
	var totalSamples int64
	sampleCountCol, _ := result.Column("sample_count")
	for i := range sampleCountCol.Len() {
		if !sampleCountCol.IsNull(i) {
			// Parse the value as string and convert to int64
			valStr := sampleCountCol.GetAsString(i)
			if count, parseErr := strconv.ParseInt(valStr, 10, 64); parseErr == nil {
				totalSamples += count
			}
		}
	}

	// Add class_percentage column
	classPercentages := make([]float64, result.Len())
	for i := range result.Len() {
		if !sampleCountCol.IsNull(i) {
			valStr := sampleCountCol.GetAsString(i)
			if count, parseErr := strconv.ParseInt(valStr, 10, 64); parseErr == nil {
				if totalSamples > 0 {
					classPercentages[i] = (float64(count) / float64(totalSamples)) * 100.0
				}
			}
		}
	}

	mem := memory.NewGoAllocator()
	percentageSeries := gorilla.NewSeries("class_percentage", classPercentages, mem)
	defer percentageSeries.Release()

	// Create a new DataFrame with the percentage column added
	columns := make([]gorilla.ISeries, 0, result.Width()+1)
	for _, colName := range result.Columns() {
		col, _ := result.Column(colName)
		columns = append(columns, col)
	}
	columns = append(columns, percentageSeries)

	finalResult := gorilla.NewDataFrame(columns...)
	result.Release() // Release the intermediate result

	return finalResult
}
