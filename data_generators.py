"""
Extended data generators for more realistic and complex data patterns.
"""

import random
import string
import math
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import numpy as np
from faker import Faker


class AdvancedDataGenerator:
    """Extended data generator with more realistic data patterns."""
    
    def __init__(self, random_seed: Optional[int] = None, locale: str = 'en_US'):
        if random_seed is not None:
            random.seed(random_seed)
            np.random.seed(random_seed)
        
        self.fake = Faker(locale)
        if random_seed is not None:
            Faker.seed(random_seed)
    
    def generate_realistic_names(self, num_rows: int, name_type: str = 'full') -> List[str]:
        """Generate realistic names using Faker."""
        if name_type == 'first':
            return [self.fake.first_name() for _ in range(num_rows)]
        elif name_type == 'last':
            return [self.fake.last_name() for _ in range(num_rows)]
        elif name_type == 'company':
            return [self.fake.company() for _ in range(num_rows)]
        else:  # full name
            return [self.fake.name() for _ in range(num_rows)]
    
    def generate_emails(self, num_rows: int, domains: Optional[List[str]] = None) -> List[str]:
        """Generate realistic email addresses."""
        if domains is None:
            domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'company.com', 'example.org']
        
        emails = []
        for _ in range(num_rows):
            username = self.fake.user_name()
            domain = random.choice(domains)
            emails.append(f"{username}@{domain}")
        
        return emails
    
    def generate_phone_numbers(self, num_rows: int, format_pattern: str = 'US') -> List[str]:
        """Generate phone numbers in various formats."""
        if format_pattern == 'US':
            return [self.fake.phone_number() for _ in range(num_rows)]
        elif format_pattern == 'international':
            return [self.fake.phone_number() for _ in range(num_rows)]
        else:
            return [self.fake.phone_number() for _ in range(num_rows)]
    
    def generate_addresses(self, num_rows: int, component: str = 'full') -> List[str]:
        """Generate address components."""
        if component == 'street':
            return [self.fake.street_address() for _ in range(num_rows)]
        elif component == 'city':
            return [self.fake.city() for _ in range(num_rows)]
        elif component == 'state':
            return [self.fake.state() for _ in range(num_rows)]
        elif component == 'zipcode':
            return [self.fake.zipcode() for _ in range(num_rows)]
        elif component == 'country':
            return [self.fake.country() for _ in range(num_rows)]
        else:  # full address
            return [self.fake.address().replace('\n', ', ') for _ in range(num_rows)]
    
    def generate_text_data(self, num_rows: int, text_type: str = 'sentence', 
                          min_length: int = 5, max_length: int = 50) -> List[str]:
        """Generate various types of text data."""
        if text_type == 'word':
            return [self.fake.word() for _ in range(num_rows)]
        elif text_type == 'sentence':
            return [self.fake.sentence() for _ in range(num_rows)]
        elif text_type == 'paragraph':
            return [self.fake.paragraph() for _ in range(num_rows)]
        elif text_type == 'random_string':
            return [''.join(random.choices(string.ascii_letters + string.digits, 
                                         k=random.randint(min_length, max_length)))
                   for _ in range(num_rows)]
        else:
            return [self.fake.text(max_nb_chars=max_length) for _ in range(num_rows)]
    
    def generate_financial_data(self, num_rows: int, data_type: str = 'amount') -> List[Any]:
        """Generate financial-related data."""
        if data_type == 'amount':
            return [round(random.uniform(0.01, 10000.00), 2) for _ in range(num_rows)]
        elif data_type == 'currency_code':
            currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF']
            return [random.choice(currencies) for _ in range(num_rows)]
        elif data_type == 'credit_card':
            return [self.fake.credit_card_number() for _ in range(num_rows)]
        elif data_type == 'iban':
            return [self.fake.iban() for _ in range(num_rows)]
        else:
            return [round(random.uniform(0.01, 1000.00), 2) for _ in range(num_rows)]
    
    def generate_time_series(self, num_rows: int, start_date: str, frequency: str = 'daily',
                           trend: str = 'none', seasonality: bool = False) -> List[datetime]:
        """Generate time series data with trends and seasonality."""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        
        if frequency == 'hourly':
            delta = timedelta(hours=1)
        elif frequency == 'daily':
            delta = timedelta(days=1)
        elif frequency == 'weekly':
            delta = timedelta(weeks=1)
        elif frequency == 'monthly':
            delta = timedelta(days=30)  # Approximate
        else:
            delta = timedelta(days=1)
        
        dates = []
        current_date = start
        
        for i in range(num_rows):
            # Add some randomness to the frequency
            if frequency != 'hourly':
                jitter = timedelta(hours=random.randint(-12, 12))
                dates.append(current_date + jitter)
            else:
                dates.append(current_date)
            
            current_date += delta
        
        return sorted(dates)
    
    def generate_correlated_data(self, base_data: List[float], correlation: float = 0.7,
                               noise_level: float = 0.1) -> List[float]:
        """Generate data that's correlated with existing data."""
        correlated_data = []
        
        for value in base_data:
            # Create correlated value with some noise
            correlated_value = value * correlation + random.gauss(0, noise_level * abs(value))
            correlated_data.append(correlated_value)
        
        return correlated_data
    
    def generate_categorical_with_hierarchy(self, num_rows: int, 
                                          hierarchy: Dict[str, List[str]]) -> List[Dict[str, str]]:
        """Generate hierarchical categorical data (e.g., category -> subcategory)."""
        results = []
        
        for _ in range(num_rows):
            # Pick a top-level category
            category = random.choice(list(hierarchy.keys()))
            # Pick a subcategory
            subcategory = random.choice(hierarchy[category])
            
            results.append({
                'category': category,
                'subcategory': subcategory
            })
        
        return results
    
    def generate_geospatial_data(self, num_rows: int, region: str = 'US') -> List[Dict[str, float]]:
        """Generate latitude/longitude coordinates."""
        coordinates = []
        
        for _ in range(num_rows):
            if region == 'US':
                # Approximate US bounds
                lat = random.uniform(25.0, 49.0)
                lon = random.uniform(-125.0, -66.0)
            elif region == 'Europe':
                lat = random.uniform(35.0, 71.0)
                lon = random.uniform(-10.0, 40.0)
            else:  # World
                lat = random.uniform(-90.0, 90.0)
                lon = random.uniform(-180.0, 180.0)
            
            coordinates.append({
                'latitude': round(lat, 6),
                'longitude': round(lon, 6)
            })
        
        return coordinates
    
    def generate_log_normal_data(self, num_rows: int, mu: float = 0.0, 
                               sigma: float = 1.0) -> List[float]:
        """Generate log-normal distributed data (good for sizes, durations, etc.)."""
        return np.random.lognormal(mu, sigma, num_rows).tolist()
    
    def generate_exponential_data(self, num_rows: int, scale: float = 1.0) -> List[float]:
        """Generate exponentially distributed data (good for wait times, etc.)."""
        return np.random.exponential(scale, num_rows).tolist()
    
    def generate_poisson_data(self, num_rows: int, lam: float = 1.0) -> List[int]:
        """Generate Poisson distributed data (good for counts, events, etc.)."""
        return np.random.poisson(lam, num_rows).tolist()
    
    def generate_seasonal_pattern(self, num_rows: int, base_value: float = 100.0,
                                amplitude: float = 20.0, period: int = 365) -> List[float]:
        """Generate data with seasonal patterns."""
        data = []
        for i in range(num_rows):
            seasonal_component = amplitude * math.sin(2 * math.pi * i / period)
            noise = random.gauss(0, base_value * 0.05)  # 5% noise
            value = base_value + seasonal_component + noise
            data.append(max(0, value))  # Ensure non-negative
        
        return data
    
    def generate_zipf_distribution(self, num_rows: int, num_categories: int = 100,
                                 alpha: float = 1.0) -> List[int]:
        """Generate data following Zipf distribution (power law)."""
        # Generate Zipf distribution
        ranks = np.arange(1, num_categories + 1)
        probabilities = 1.0 / (ranks ** alpha)
        probabilities /= probabilities.sum()
        
        return np.random.choice(ranks, size=num_rows, p=probabilities).tolist()


class DataRelationshipManager:
    """Manages relationships and dependencies between columns."""
    
    def __init__(self):
        self.relationships = {}
    
    def add_relationship(self, dependent_column: str, source_column: str, 
                        relationship_type: str, **kwargs):
        """Add a relationship between columns."""
        self.relationships[dependent_column] = {
            'source': source_column,
            'type': relationship_type,
            'params': kwargs
        }
    
    def apply_relationships(self, data: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
        """Apply all defined relationships to the data."""
        for dependent_col, relationship in self.relationships.items():
            source_col = relationship['source']
            rel_type = relationship['type']
            params = relationship['params']
            
            if source_col in data:
                if rel_type == 'derived_categorical':
                    data[dependent_col] = self._derive_categorical(
                        data[source_col], params.get('mapping', {}))
                elif rel_type == 'derived_numeric':
                    data[dependent_col] = self._derive_numeric(
                        data[source_col], params.get('formula', lambda x: x))
                elif rel_type == 'conditional':
                    data[dependent_col] = self._apply_conditional(
                        data[source_col], params.get('conditions', {}))
        
        return data
    
    def _derive_categorical(self, source_data: List[Any], mapping: Dict[Any, Any]) -> List[Any]:
        """Derive categorical data based on source values."""
        return [mapping.get(value, 'Unknown') for value in source_data]
    
    def _derive_numeric(self, source_data: List[Any], formula) -> List[Any]:
        """Derive numeric data using a formula."""
        return [formula(value) for value in source_data]
    
    def _apply_conditional(self, source_data: List[Any], conditions: Dict[Any, Any]) -> List[Any]:
        """Apply conditional logic to derive values."""
        result = []
        for value in source_data:
            for condition, result_value in conditions.items():
                if callable(condition):
                    if condition(value):
                        result.append(result_value)
                        break
                elif value == condition:
                    result.append(result_value)
                    break
            else:
                result.append(conditions.get('default', None))
        
        return result
