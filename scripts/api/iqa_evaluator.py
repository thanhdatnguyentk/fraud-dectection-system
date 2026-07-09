import cv2
import numpy as np
from typing import Dict, Tuple, Optional

class IQAEvaluator:
    """
    Image Quality Assessment (IQA) Evaluator for Bill/Receipt Images.
    Maps physical image metrics to Business Rules (Rule 2026).
    """
    
    def __init__(self):
        # Recommended thresholds based on EDA of 500 images
        self.THRESHOLDS = {
            'blur_min': 80.4,
            'contrast_min': 41.3,
            'brightness_min': 106.1,
            'brightness_max': 184.3,
            'skew_max': 25.1,
            'glare_max_pct': 15.8
        }
        # Built-in OpenCV QR Detector for IQ007/IQ008
        self.qr_detector = cv2.QRCodeDetector()

    def _get_skew_angle(self, gray_img: np.ndarray) -> float:
        """Calculate skew angle using Hough Lines."""
        edges = cv2.Canny(gray_img, 50, 150, apertureSize=3)
        lines = cv2.HoughLinesP(edges, 1, np.pi/180, 100, minLineLength=100, maxLineGap=10)
        
        if lines is None:
            return 0.0
            
        angles = []
        for line in lines:
            x1, y1, x2, y2 = line[0]
            angle = np.degrees(np.arctan2(y2 - y1, x2 - x1))
            # normalize to [0, 90] deviation
            angle = abs(angle) % 90
            if angle > 45:
                angle = 90 - angle
            angles.append(angle)
            
        return float(np.median(angles)) if angles else 0.0

    def evaluate(self, image_path: str) -> Dict:
        """
        Evaluate image quality and map to Rule 2026.
        Returns a dictionary with metrics and rule violations.
        """
        img = cv2.imread(image_path)
        if img is None:
            return {"error": "Could not read image"}
            
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # 1. Calculate Metrics
        blur_score = cv2.Laplacian(gray, cv2.CV_64F).var()
        brightness = gray.mean()
        contrast = gray.std()
        glare_pct = (np.sum(gray > 240) / gray.size) * 100
        skew_angle = self._get_skew_angle(gray)
        
        # 2. Check QR Code (IQ007, IQ008)
        # Scan the bottom half of the bill usually for QR
        h, w = gray.shape
        footer_crop = gray[int(h*0.5):, :]
        qr_data, bbox, _ = self.qr_detector.detectAndDecode(footer_crop)
        qr_detected = bbox is not None
        qr_decoded = bool(qr_data)

        # 3. Map to Rules
        violations = []
        warnings = []
        
        # IT001 / IT002: Blur Check
        if blur_score < self.THRESHOLDS['blur_min']:
            violations.append({
                "rule": "IT001/IT002",
                "reason": f"Blur score {blur_score:.1f} < threshold {self.THRESHOLDS['blur_min']} (Image too blurry)"
            })
            
        # IL001 / IL002: Skew Check
        if skew_angle > self.THRESHOLDS['skew_max']:
            violations.append({
                "rule": "IL001/IL002",
                "reason": f"Skew angle {skew_angle:.1f}° > threshold {self.THRESHOLDS['skew_max']}° (Risk of line grouping failure)"
            })
            
        # Contrast / Illumination Warning
        if contrast < self.THRESHOLDS['contrast_min']:
            warnings.append(f"Low contrast: {contrast:.1f}. Requires Image Enhancement (CLAHE).")
            
        # IQ007 / IQ008: QR Validation
        if not qr_detected:
            # Note: Not all bills have QR, but if it's mandatory, it hits IQ007
            warnings.append("IQ007: QR Code not detected in footer.")
        elif qr_detected and not qr_decoded:
            violations.append({
                "rule": "IQ008",
                "reason": "QR Code detected but could not be decoded (Damage or Blur)."
            })
            
        is_valid = len(violations) == 0

        return {
            "is_valid": is_valid,
            "metrics": {
                "blur": round(blur_score, 2),
                "brightness": round(brightness, 2),
                "contrast": round(contrast, 2),
                "glare_pct": round(glare_pct, 2),
                "skew_angle": round(skew_angle, 2),
                "qr_detected": qr_detected,
                "qr_decoded": qr_decoded
            },
            "violations": violations,
            "warnings": warnings
        }

if __name__ == "__main__":
    # Quick test
    evaluator = IQAEvaluator()
    print("IQAEvaluator module ready for integration.")
