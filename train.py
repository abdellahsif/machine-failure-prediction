import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE

# 1. Load the data
df = pd.read_csv('machine failure.csv')

# 2. Preprocessing
# Encode 'Type' (categorical)
le = LabelEncoder()
df['Type'] = le.fit_transform(df['Type'])  # M=2, L=1, H=0 (example)

# Drop non-feature columns
X = df.drop(columns=['UDI', 'Product ID', 'Machine failure', 'TWF', 'HDF', 'PWF', 'OSF', 'RNF'])
y = df['Machine failure']

# 3. Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# 4. Handle class imbalance
smote = SMOTE(random_state=42)
X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

# 5. Train model
clf = RandomForestClassifier(n_estimators=100, class_weight='balanced', random_state=42)
clf.fit(X_train_smote, y_train_smote)

# 6. Evaluate
y_pred = clf.predict(X_test)
print("\n🔍 Classification Report:\n")
print(classification_report(y_test, y_pred))
print("\n📊 Confusion Matrix:\n")
print(confusion_matrix(y_test, y_pred))

# 7. Save model
joblib.dump(clf, 'machine_failure_model.pkl')
print("\n✅ Model saved as 'machine_failure_model.pkl'")
